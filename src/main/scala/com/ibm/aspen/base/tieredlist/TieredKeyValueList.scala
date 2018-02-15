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
import com.ibm.aspen.core.read.ThresholdError

trait TieredKeyValueList {
  
  import TieredKeyValueList._
  
  val keyOrdering: KeyOrdering
  
  //def refresh(): Future[Unit]
  
  /** Future completes when the transactions is ready to commit */
  //def prepreUpdateRootTransaction(newRootPointer: KeyValueObjectPointer)(implicit tx: Transaction, ec: ExecutionContext): Future[Unit]
  
  /** Returns the zero-based number of tiers in the tree. */
  //def numberOfTiers: Int
  
  protected def rootPointer(): Future[(Int, KeyValueObjectPointer)]
  
  protected def getObjectReaderForTier(tier: Int): ObjectReader
  
  protected def fetchContainingNode(key: Key, targetTier: Int)(implicit ec: ExecutionContext): Future[KeyValueObjectState] = {
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
            case t: ThresholdError => navigateTier(tiers.tail, blacklist + tierPointer.pointer.uuid)
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
    
    rootPointer() onComplete {
      case Failure(cause) => p.failure(cause)
      case Success((depth, ptr)) => navigateTier( (depth, KeyValueListPointer(Key.AbsoluteMinimum, ptr)) :: Nil, Set()) 
    }
    
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
        case t: ThresholdError => p.success(Left(blacklist + listPointer.pointer.uuid))
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