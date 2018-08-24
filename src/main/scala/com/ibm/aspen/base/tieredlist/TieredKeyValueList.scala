package com.ibm.aspen.base.tieredlist

import com.ibm.aspen.core.objects.KeyValueObjectPointer
import com.ibm.aspen.base.Transaction
import scala.concurrent.ExecutionContext
import scala.concurrent.Future
import com.ibm.aspen.base.ObjectReader
import com.ibm.aspen.core.objects.KeyValueObjectState
import com.ibm.aspen.core.objects.keyvalue.KeyOrdering
import com.ibm.aspen.core.objects.keyvalue.Key
import java.util.UUID
import scala.concurrent.Promise
import com.ibm.aspen.core.objects.ObjectPointer
import scala.util.Failure
import scala.util.Success
import jdk.nashorn.internal.runtime.FindProperty
import com.ibm.aspen.core.objects.keyvalue.Value
import java.nio.ByteBuffer
import com.ibm.aspen.core.network.protocol.KeyComparison
import com.ibm.aspen.core.objects.keyvalue.ByteArrayKeyOrdering
import com.ibm.aspen.core.objects.keyvalue.IntegerKeyOrdering
import com.ibm.aspen.core.objects.keyvalue.LexicalKeyOrdering
import com.ibm.aspen.core.read.FatalReadError
import com.ibm.aspen.core.DataBuffer
import com.ibm.aspen.util.Varint


trait TieredKeyValueList {
  
  import TieredKeyValueList._
  
  val rootManager: TieredKeyValueListRootManager
 
  protected[tieredlist] def getObjectReaderForTier(tier: Int): ObjectReader
  
  def keyOrdering: KeyOrdering = rootManager.root.keyOrdering
  
  def get(key: Key)(implicit ec: ExecutionContext): Future[Option[Value]] = fetchContainingNode(key, 0) map { kvos =>
    kvos.contents.get(key) 
  }
  
  def visitAll(visitor: (Value) => Unit)(implicit ec: ExecutionContext): Future[Unit] = visitRange(Key.AbsoluteMinimum, None, visitor)
  
  def visitRange(
      startKey: Key, 
      stopKey: Option[Key], 
      visitor: (Value) => Unit)(implicit ec: ExecutionContext): Future[Unit] = {
    
    val p = Promise[Unit]()
    
    fetchContainingNode(startKey, targetTier=0) onComplete {
      case Failure(cause) => p.failure(cause)
      case Success(root) => 
        def visit(kvos: KeyValueObjectState): Unit = {
          
          val reader = getObjectReaderForTier(0)
          
          kvos.contents.valuesIterator.toArray.sortBy(v => v.key)(keyOrdering).foreach { v =>
            stopKey match {
              case None => visitor(v)
              case Some(stop) => if (keyOrdering.compare(v.key, stop) <= 0) visitor(v)
            }
          }
          
          val nextNode = kvos.right match {
            case None => None
            case Some(right) =>
              val rp = KeyValueObjectPointer(right.content)
              stopKey match {
                case None => Some(rp)
                case Some(stop) =>
                  kvos.maximum match {
                    case None => None
                    case Some(max) => if (keyOrdering.compare(stop, max.key) >= 0) Some(rp) else None
                  }
                  
              }
          }
              
          nextNode match {
            case None => p.success(())
            case Some(nextPointer) => reader.readObject(nextPointer) onComplete {
              case Failure(cause) => p.failure(cause)
              case Success(kvos) => visit(kvos)
            }
          }
        }
        
        visit(root)
    }
    p.future
  }
  
  protected[tieredlist] def fetchContainingNode(key: Key, targetTier: Int)(implicit ec: ExecutionContext): Future[KeyValueObjectState] = {
    val p = Promise[KeyValueObjectState]()
    
    def navigateTier(tiers: List[(Int, KeyValueListPointer)], blacklist: Set[UUID]): Unit = {
      
      if (tiers.isEmpty) {
        p.failure(new Exception("Broken Tiered List Navigation!"))
        return
      }
      
      val (tier, tierPointer) = tiers.head
      
      if (blacklist.contains(tierPointer.pointer.uuid)) {
          navigateTier(tiers.tail, blacklist)
          return
      }
      
      val objectReader = getObjectReaderForTier(tier)
      
      if (tier == targetTier) { 
        // Fetch the first node in the tier and scan to the correct target node. This read is the most likely failure point as we could be
        // attempting to read a node deleted by a join operation. If so, blacklist the node and resume the search from the parent tier
        objectReader.readObject(tierPointer.pointer) onComplete {
          case Failure(cause) => cause match {
            case t: FatalReadError => navigateTier(tiers.tail, blacklist + tierPointer.pointer.uuid)
            case _ => p.failure(cause)
          }
          
          case Success(initialKvos) => KeyValueList.scanToContainingNode(objectReader, initialKvos, keyOrdering, key) onComplete {
            case Failure(cause) => p.failure(cause)
            case Success(kvos) => p.success(kvos)
          }
        }
      }
      else {
        findPointerToNextTierDown(objectReader, tierPointer, keyOrdering, blacklist, key) onComplete {
          case Failure(cause) => p.failure(cause)
          
          case Success(e) => e match {
            case Left(newBlacklist) =>  navigateTier(tiers.tail, newBlacklist)
              
            case Right(lowerTierPtr) => navigateTier( (tier-1, lowerTierPtr) :: tiers, blacklist)
          }
        }
      }
    }
    
    navigateTier((rootManager.root.topTier, KeyValueListPointer(Key.AbsoluteMinimum, rootManager.root.rootNode)) :: Nil, Set()) 
    
    p.future
  }
}

object TieredKeyValueList {
  
  def findPointerToNextTierDown(
      objectReader: ObjectReader, 
      listPointer: KeyValueListPointer, 
      ordering: KeyOrdering,
      blacklist: Set[UUID],
      key: Key)(implicit ec: ExecutionContext) : Future[Either[Set[UUID], KeyValueListPointer]] = {
    
    val p = Promise[Either[Set[UUID], KeyValueListPointer]]()
    
    // This read is the most likely failure point. We could attempt to read a node that has been deleted via a join operation
    objectReader.readObject(listPointer.pointer) onComplete {
      case Failure(cause) => cause match {
        case t: FatalReadError => p.success(Left(blacklist + listPointer.pointer.uuid))
        case _ => p.failure(cause)
      }
      
      case Success(initialKvos) => KeyValueList.scanToContainingNode(objectReader, initialKvos, ordering, key, blacklist) onComplete {
        case Failure(cause) => p.failure(cause)
        
        case Success(okvos) => okvos match {
          case None => p.success(Left(blacklist + listPointer.pointer.uuid))
          
          case Some(kvos) =>
            val init: Option[KeyValueListPointer] = None
            
            val optr = kvos.contents.foldLeft(init){ (o, t) => o match {
              case None => if (ordering.compare(t._1, key) > 0) None else {
                val ptr = ObjectPointer.fromArray(t._2.value).asInstanceOf[KeyValueObjectPointer]
                if (blacklist.contains(ptr.uuid)) None else Some(KeyValueListPointer(t._1, ptr))
              }
              
              case Some(p) => if (ordering.compare(t._1, key) <= 0 && ordering.compare(t._1, p.minimum) > 0) {
                val ptr = ObjectPointer.fromArray(t._2.value).asInstanceOf[KeyValueObjectPointer]
                if (blacklist.contains(ptr.uuid)) Some(p) else Some(KeyValueListPointer(t._1, ptr))
              } else
                Some(p)
            }}
            
            optr match {
              case None => p.success(Left(blacklist + kvos.pointer.uuid))
              case Some(ptr) => p.success(Right(ptr))
            }
        }
      }
    }
    
    p.future
  }
  
}