package com.ibm.aspen.base.kvtree

import com.ibm.aspen.core.objects.ObjectPointer
import com.ibm.aspen.core.objects.ObjectRevision
import java.util.UUID
import com.ibm.aspen.base.AspenSystem
import scala.concurrent.ExecutionContext
import scala.concurrent.Future
import scala.concurrent.Promise
import scala.util.Success
import scala.util.Failure
import java.nio.ByteBuffer

private[kvtree] abstract class KVTreeImpl(
    val treeDescriptionPointer: ObjectPointer, // Pointer to object holding the serialized content of this instance
    private[kvtree] val objectAllocater: KVTreeObjectAllocater,
    val system: AspenSystem,
    val compareKeys: (Array[Byte], Array[Byte]) => Int,
    private[this] var treeDescriptionRevision: ObjectRevision,
    private[this] var tierPointers: List[ObjectPointer]) extends KVTree {
  
  // Use a reversed list internally. We'll generally only want the root pointer
  tierPointers = tierPointers.reverse
  
  private[this] var initializationFuture: Option[Future[ObjectPointer]] = None
  
  def tierSizelimit(tier: Int): Int = objectAllocater.tierSizelimit(tier)
      
  // Minimum for a node in the left-most tier is always  new Array[]()
   
  def refresh()(implicit ec: ExecutionContext): Future[Unit] = system.readObject(treeDescriptionPointer, None) map { osd =>
    val (_, tiers) = KVTreeCodec.decodeTreeDescription(osd.data)
    synchronized {
      if (osd.revision > treeDescriptionRevision) {
        tierPointers = tiers.reverse
        treeDescriptionRevision = osd.revision 
      }
    }
  }
 
  private[this] def allocateInitialNode()(implicit ec: ExecutionContext): Future[ObjectPointer] = synchronized {
    initializationFuture match {
      case Some(f) => f
      case None =>
        val p = Promise[ObjectPointer]()
        initializationFuture = Some(p.future)
        
        implicit val tx = system.newTransaction()
        
        objectAllocater.allocate(treeDescriptionPointer, treeDescriptionRevision, 0, ByteBuffer.allocate(0)) onComplete {
          
          case Success(ptr) => synchronized {
            val data = KVTreeCodec.encodeTreeDescription(objectAllocater.allocationPolicyUUID, (ptr :: tierPointers).reverse)
            tx.overwrite(treeDescriptionPointer, treeDescriptionRevision, data)
            tx.commit() onComplete {
              case Success(_) =>
                tierPointers = ptr :: tierPointers
                p.success(ptr)
              case Failure(reason) => p.failure(reason)
            }
          }
          
          case Failure(reason) => synchronized { 
            tx.invalidateTransaction(reason)
            p.failure(reason)
          }
        }
        
        p.future
    }
  }
  
  def getRootPointer()(implicit ec: ExecutionContext): Future[ObjectPointer] = synchronized {
    tierPointers match {
      case Nil => allocateInitialNode()
      case lst => Future.successful(lst.head)
    }
  }
  
} 
 