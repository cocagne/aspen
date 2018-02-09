package com.ibm.aspen.base.keyvalue

import com.ibm.aspen.base.AspenSystem
import com.ibm.aspen.core.objects.keyvalue.Key
import scala.concurrent.Future
import scala.concurrent.ExecutionContext
import com.ibm.aspen.core.objects.KeyValueObjectState
import com.ibm.aspen.base.ObjectReader
import scala.concurrent.Promise
import com.ibm.aspen.core.objects.keyvalue.KeyComparison
import com.ibm.aspen.core.objects.KeyValueObjectPointer
import scala.util.Failure
import scala.util.Success
import com.ibm.aspen.core.objects.ObjectPointer
import com.ibm.aspen.base.ObjectAllocater
import com.ibm.aspen.base.Transaction
import com.ibm.aspen.core.objects.keyvalue.Insert
import com.ibm.aspen.core.objects.keyvalue.KeyValueOperation
import com.ibm.aspen.core.objects.keyvalue.Delete
import com.ibm.aspen.core.objects.keyvalue.KeyValueObjectCodec
import com.ibm.aspen.core.objects.keyvalue.SetMin
import com.ibm.aspen.core.objects.keyvalue.SetMax
import com.ibm.aspen.core.objects.keyvalue.SetLeft
import com.ibm.aspen.core.objects.keyvalue.SetRight
import com.ibm.aspen.core.objects.keyvalue.Value
import com.ibm.aspen.core.transaction.KeyValueUpdate


object KeyValueList {
  
  def fetchContainingNode(
      objectReader: ObjectReader, 
      listPointer: KeyValueListPointer, 
      comparison: KeyComparison,
      key: Key)(implicit ec: ExecutionContext) : Future[KeyValueObjectState] = {
    
    // exit immediately if the requested key is below the minimum range
    if (comparison(key, listPointer.minimum) < 0)
      return Future.failed(new BelowMinimumError(listPointer.minimum, key))
     
    val p = Promise[KeyValueObjectState]()
   
    def scanToContainingNode(pointer: KeyValueObjectPointer): Unit = objectReader.readObject(pointer) onComplete {
      case Failure(err) => p.failure(err)
      case Success(kvos) => 
        if (kvos.keyInRange(key, comparison))
          p.success(kvos)
        else {
          kvos.right match {
            case None => p.failure(new CorruptedLinkedList)
            case Some(arr) => try {
              scanToContainingNode( ObjectPointer.fromArray(arr).asInstanceOf[KeyValueObjectPointer] )
            } catch {
              case err: Throwable => p.failure(new CorruptedLinkedList)
            }
          }
        }
    }
   
    scanToContainingNode(listPointer.pointer)
   
    p.future
  }
  
  def prepreUpdateTransaction(
      kvos: KeyValueObjectState,
      nodeSizeLimit: Int,
      inserts: List[(Key, Array[Byte])],
      deletes: List[Key],
      requirements: List[KeyValueUpdate.KVRequirement],
      comparison: KeyComparison,
      reader: ObjectReader,
      allocater: ObjectAllocater,
      onSplit: (KeyValueObjectPointer) => Unit,
      onJoin: (KeyValueObjectPointer) => Unit)(implicit tx: Transaction, ec: ExecutionContext): Future[KeyValueObjectState] = {
    
    if (inserts.exists(t => !kvos.keyInRange(t._1, comparison)) || deletes.exists(key => !kvos.keyInRange(key, comparison)))
       return Future.failed(new OutOfRange)
   
    val timestamp = tx.timestamp()
    
    val appendOps = (inserts.iterator.map(t => new Insert(t._1.bytes, t._2, timestamp)) ++ deletes.iterator.map(key => new Delete(key.bytes))).toList
    
    val maxSize = kvos.pointer.size match {
      case None => nodeSizeLimit
      case Some(lim) => if (lim < nodeSizeLimit) lim else nodeSizeLimit
    }
    
    val sizeAfterAppend = kvos.sizeOnStore + KeyValueObjectCodec.calculateEncodedSize(kvos.pointer.ida, appendOps) 
    
    if (sizeAfterAppend <= maxSize) {
      tx.append(kvos.pointer, None, requirements, appendOps)
      Future.successful(updateState(kvos, appendOps, sizeAfterAppend, kvos.maximum, kvos.right))
    } else {
      
      val deleteSet = deletes.toSet
      var ops = List[KeyValueOperation]()
      
      kvos.minimum.foreach( arr => ops = new SetMin(arr) :: ops )
      kvos.maximum.foreach( arr => ops = new SetMax(arr) :: ops )
      kvos.left.foreach( arr => ops = new SetLeft(arr) :: ops )
      kvos.right.foreach( arr => ops = new SetRight(arr) :: ops )
      
      inserts.foreach(t => ops = new Insert(t._1.bytes, t._2, timestamp) :: ops)
      kvos.contents.foreach { t => 
        if (!deleteSet.contains(t._1))
          ops = new Insert(t._1.bytes, t._2.value, t._2.timestamp) :: ops
      }
      
      if (KeyValueObjectCodec.calculateEncodedSize(kvos.pointer.ida, ops) <= maxSize) {
        tx.overwrite(kvos.pointer, kvos.revision, requirements, ops)
        Future.successful(updateState(kvos, appendOps, sizeAfterAppend, kvos.maximum, kvos.right))
      } else
        split(kvos, requirements, nodeSizeLimit, inserts, deletes, comparison, allocater, onSplit)
    }
  }
  
  private def updateState(
      kvos: KeyValueObjectState, 
      ops: List[KeyValueOperation], 
      newSizeOnStore: Int, 
      maximum: Option[Key], 
      right: Option[Array[Byte]]): KeyValueObjectState = {
    
    val newContents = ops.foldLeft(kvos.contents) { (m, op) => op match {
      case i: Insert =>
        val key = Key(i.key)
        m + (key -> Value(key, i.value, i.timestamp))
      case d: Delete => m - Key(d.value)
      case _ => m
    }}
    
    new KeyValueObjectState(kvos.pointer, kvos.revision, kvos.refcount, kvos.timestamp, newSizeOnStore, 
        kvos.minimum, maximum, kvos.left, right, newContents)
  }
   
  private def split(
      kvos: KeyValueObjectState,
      requirements: List[KeyValueUpdate.KVRequirement],
      nodeSizeLimit: Int,
      inserts: List[(Key, Array[Byte])],
      deletes: List[Key],
      comparison: KeyComparison,
      allocater: ObjectAllocater,
      onSplit: (KeyValueObjectPointer) => Unit)(implicit tx: Transaction, ec: ExecutionContext): Future[KeyValueObjectState] = {
    Future.failed(new Exception("TODO"))
  }
}