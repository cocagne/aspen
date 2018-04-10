package com.ibm.aspen.base.tieredlist

import java.util.UUID
import com.ibm.aspen.base.Transaction
import com.ibm.aspen.core.objects.keyvalue.Key
import com.ibm.aspen.core.objects.KeyValueObjectPointer
import scala.concurrent.Future
import com.ibm.aspen.base.impl.BaseCodec
import com.ibm.aspen.base.RetryStrategy
import com.ibm.aspen.base.AspenSystem
import com.ibm.aspen.base.FinalizationActionHandler
import com.ibm.aspen.base.FinalizationAction
import scala.concurrent.ExecutionContext
import com.ibm.aspen.core.objects.keyvalue.KeyOrdering
import scala.util.Failure
import scala.util.Success
import com.ibm.aspen.core.objects.keyvalue.Delete
import com.ibm.aspen.core.objects.KeyValueObjectState
import com.ibm.aspen.core.objects.ObjectPointer
import com.ibm.aspen.core.transaction.KeyValueUpdate.KVRequirement
import com.ibm.aspen.core.transaction.KeyValueUpdate
import scala.concurrent.Promise


object TieredKeyValueListJoinFA {
  val FinalizationActionUUID = UUID.fromString("fb262a22-8aea-4bbd-8da8-9c3a939c3786")
  
  /** 
   * @param transaction Transaction to add the FinalizationAction to
   * @param treeIdentifier Unique identifer for the tree root key-value pair
   * @param treeContainer Either a stand-alone key-value object that contains the tree root or a tiered list that contains the key
   * @param keyOrdering Defines key comparisons for sorting
   * @param targetTier the tier number into which the new node pointer should be inserted
   * @param left Pointer to the node to the left of the one removed
   * @param removed Pointer to the removed node
   */
  def addFinalizationAction(
      transaction: Transaction, 
      treeIdentifier: Key, 
      treeContainer: Either[KeyValueObjectPointer, TieredKeyValueList.Root],
      keyOrdering: KeyOrdering,
      targetTier: Int,
      left: KeyValueListPointer,
      removed: KeyValueListPointer): Unit = {
    
    // TODO: Support removal of the root node. For now we'll skip join finalizers for all left-most nodes. Tree structure and depth is preserved
    //       we just loose the uncached lookup efficiency typically found in smaller trees.  
    if ( removed.minimum != KeyValueListPointer.AbsoluteMinimum ) {
      val serializedContent = BaseCodec.encodeTieredKeyValueListJoinFA(treeIdentifier, treeContainer, keyOrdering, targetTier, left, removed)
          
      transaction.addFinalizationAction(FinalizationActionUUID, serializedContent)
    }
  }
  
  case class Content(
      treeIdentifier: Key, 
      treeContainer: Either[KeyValueObjectPointer, TieredKeyValueList.Root], 
      targetTier: Int,
      left: KeyValueListPointer,
      removed: KeyValueListPointer,
      keyOrdering: KeyOrdering) 
}

class TieredKeyValueListJoinFA(
    val system: AspenSystem) extends FinalizationActionHandler {
  
  import TieredKeyValueListJoinFA._
  
  val typeUUID: UUID = FinalizationActionUUID
  
  def createAction(serializedActionData: Array[Byte]): FinalizationAction = {
    new RemoveFromUpperTier( BaseCodec.decodeTieredKeyValueListJoinFA(serializedActionData) )
  }
  
  class RemoveFromUpperTier(val c: Content) extends FinalizationAction {
    
    def completionDetected(): Unit = ()
    
    def execute()(implicit ec: ExecutionContext): Future[Unit] = system.retryStrategy.retryUntilSuccessful {
      
      val lst = new SimpleMutableTieredKeyValueList(system, c.treeContainer, c.treeIdentifier, c.keyOrdering)
      
      def remove(root: TieredKeyValueList.Root): Future[Unit] = system.transact { implicit tx =>
        
        def onSplit(left: KeyValueListPointer, right: KeyValueListPointer): Unit = {}
        
        def onJoin(left: KeyValueListPointer, removed: KeyValueListPointer): Unit = {
          addFinalizationAction(tx, c.treeIdentifier, c.treeContainer, c.keyOrdering, c.targetTier+1, left, removed)
        }
        
        for {
          kvos <- lst.fetchContainingNode(c.removed.minimum, c.targetTier) 
          
          ovalue = kvos.contents.get(c.removed.minimum)
          
          if ovalue.isDefined
          
          value = ovalue.get
          
          if c.removed.pointer == ObjectPointer.fromArray(value.value)
          
          allocater <- system.getObjectAllocater(root.getTierNodeAllocaterUUID(c.targetTier))
          
          nodeSizeLimit = root.getTierNodeSize(c.targetTier)
          inserts = Nil
          deletes = List(c.removed.minimum)
          requirements = List(KVRequirement(c.removed.minimum, value.timestamp, KeyValueUpdate.TimestampRequirement.Equals))
          
          _ = tx.ensureHappensAfter(value.timestamp)
          
          test <- system.readObject(kvos.pointer)

          ready <- KeyValueList.prepreUpdateTransaction(kvos, nodeSizeLimit, inserts, deletes, requirements, c.keyOrdering, system, allocater, onSplit, onJoin)
          
          done <- tx.commit()

        } yield ()
      }
      
      val p = Promise[Unit]()
      
      lst.refreshRoot() onComplete {
        case Success((_, root)) => p.completeWith(remove(root))
        
        case Failure(_) =>
          // Failures here must be either due to the containing object being corrupt/deleted or the root key
          // being deleted. In either case there's nothing we can do to recover. Declare success.
          p.success(())
      }
      
      p.future
    }    
  }
}