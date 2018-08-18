package com.ibm.aspen.core.objects.keyvalue

import com.ibm.aspen.core.DataBuffer
import com.ibm.aspen.core.ida.IDA
import com.ibm.aspen.util.Varint
import java.util.UUID
import java.nio.ByteBuffer
import com.ibm.aspen.core.ida.Replication
import com.ibm.aspen.core.objects.ObjectPointer
import com.ibm.aspen.core.objects.ObjectRevision
import com.ibm.aspen.core.HLCTimestamp
import com.ibm.aspen.core.objects.ObjectRefcount
import com.ibm.aspen.core.objects.KeyValueObjectState
import com.ibm.aspen.core.objects.ObjectEncodingError
import com.ibm.aspen.core.objects.KeyValueObjectPointer
    
object KeyValueObjectCodec {
  
  def getHigestRevisionCount(l: List[ObjectRevision]): (ObjectRevision, Int) = {
    val m = l.foldLeft(Map[ObjectRevision, Int]()){ (m, r) => m.get(r) match { 
      case Some(count) => m + (r -> (count + 1))
      case None => m + (r -> 1)
    }}
    
    m.foldLeft((ObjectRevision.Null, 0))( (x, t) => if (t._2 > x._2) t else x )
  }
  
  def getRecoverableRevision(ida: IDA, l: List[ObjectRevision]): Option[ObjectRevision] = {
    val (revision, count) = getHigestRevisionCount(l)
    
    if (count >= ida.consistentRestoreThreshold) Some(revision) else None
  }
  
  /** Converts to Map[(item, count)] then selects the item with the highest count */
  def getCurrentReplicatedValue[T](l: List[T]): T = {
    val m = l.foldLeft(Map[T,Int]()) { (m, i) => m.get(i) match {
      case None => m + (i -> 1)
      case Some(c) => m + (i -> (c+1))
    }}
    m.foldLeft(m.head)((x,t) => if (t._2 > x._2) t else x)._1
  }
  
  def getDecodeableRevision(
      restoreThreshold: Int, 
      l: List[(Int, ObjectRevision, HLCTimestamp, Array[Byte])]): Option[(ObjectRevision, HLCTimestamp, List[(Byte, Array[Byte])])] = {
    l.foldLeft(Map[ObjectRevision,Int]()) { (m, i) => m.get(i._2) match {
      case None => m + (i._2 -> 1)
      case Some(c) => m + (i._2 -> (c+1))
    }}.find(t => t._2 > restoreThreshold).map { t =>
      val sub = l.filter(i => i._2 == t._1)
      (t._1, sub.head._3, sub.map(i => (i._1.asInstanceOf[Byte], i._4)))
    }
  }
  
  def restore(
      ida: IDA, 
      l: List[(Int, ObjectRevision, HLCTimestamp, Array[Byte])]): Option[(ObjectRevision, HLCTimestamp, Array[Byte])] = {
    getDecodeableRevision(ida.restoreThreshold, l).map { t =>
      (t._1, t._2, ida.restoreArray(t._3))
    }
  }
  
  /** Returns True when either we have enough responses to restore the KVPair or it is impossible to restore the KVPair (deleted pair) */
  def isRestorable(l: List[ObjectRevision], numResponses: Int, ida: IDA): Boolean = {
    val potentialResponses = ida.width - numResponses
    val (_, count) = getHigestRevisionCount(l)
    count > ida.restoreThreshold || count + potentialResponses < ida.restoreThreshold
  }
  
  def isRestorable(ida: IDA, storeStates: List[KeyValueObjectStoreState]): Boolean = {
    val numResponses = storeStates.size
    isRestorable(storeStates.filter(_.minimum.isDefined).map(_.minimum.get.revision), numResponses, ida) &&
    isRestorable(storeStates.filter(_.maximum.isDefined).map(_.maximum.get.revision), numResponses, ida) &&
    isRestorable(storeStates.filter(_.left.isDefined).map(_.left.get.revision), numResponses, ida) &&
    isRestorable(storeStates.filter(_.right.isDefined).map(_.right.get.revision), numResponses, ida) && {
      storeStates.foldLeft(Map[Key, List[ObjectRevision]]()) { (m, kvoss) =>
        kvoss.idaEncodedContents.foldLeft(m) { (x, t) =>
          val lst = x.get(t._1) match {
            case None => t._2.revision :: Nil
            case Some(l) => t._2.revision :: l
          }
          x + (t._1 -> lst)
        }
      }.forall(t => isRestorable(t._2, numResponses, ida))
    }
  }
  
  /** storeStates - List[(ida-encoding-index, store-state)]
   */
  def decode(
      pointer: KeyValueObjectPointer, 
      revision:ObjectRevision,
      refcount:ObjectRefcount, 
      timestamp: HLCTimestamp,
      readTimestamp: HLCTimestamp,
      storeStates: List[(Int, KeyValueObjectStoreState)]): KeyValueObjectState = {
    
    val ida = pointer.ida
    
    try {
      
      // Replicated value so whichever has the highest occurrence must be the current value
      val minimum = getCurrentReplicatedValue(storeStates.map(_._2.minimum)).map(m => KeyValueObjectState.Min(m.key, m.revision, m.timestamp))
      val maximum = getCurrentReplicatedValue(storeStates.map(_._2.maximum)).map(m => KeyValueObjectState.Max(m.key, m.revision, m.timestamp))
      
      val left = restore(ida, storeStates.filter(t => t._2.left.isDefined).map { t =>
        val l = t._2.left.get
        (t._1, l.revision, l.timestamp, l.idaEncodedContent)
      }).map(t => KeyValueObjectState.Left(t._3, t._1, t._2))
      
      val right = restore(ida, storeStates.filter(t => t._2.right.isDefined).map { t =>
        val r = t._2.right.get
        (t._1, r.revision, r.timestamp, r.idaEncodedContent)
      }).map(t => KeyValueObjectState.Right(t._3, t._1, t._2))
      
      val kvStates = storeStates.foldLeft(Map[Key, List[(Int, ObjectRevision, HLCTimestamp, Array[Byte])]]()) { (m, t) =>
        val (idaIndex, kvoss) = t
        kvoss.idaEncodedContents.foldLeft(m) { (x, t) => 
          val li = (idaIndex, t._2.revision, t._2.timestamp, t._2.value)
          val lst = x.get(t._1) match {
            case None => li :: Nil
            case Some(l) => li :: l
          }
          x + (t._1 -> lst)
        }
      }
      
      val contents = kvStates.foldLeft(Map[Key, Value]()) { (m, t) =>
        val (key, lst) = t
        restore(ida, lst) match {
          case None => m
          case Some(r) => m + (key -> Value(key, r._3, r._2, r._1))
        }
      }

      new KeyValueObjectState(pointer, revision, refcount, timestamp, readTimestamp, minimum, maximum, left, right, contents)
    } catch {
      case t: Throwable => throw new KeyValueObjectEncodingError(t)
    }
  }
  
}