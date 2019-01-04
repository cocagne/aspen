package com.ibm.aspen.base.tieredlist

import java.util.UUID

import com.ibm.aspen.base.{AspenSystem, FinalizationAction, FinalizationActionHandler, Transaction}
import com.ibm.aspen.base.impl.BaseCodec
import com.ibm.aspen.core.objects.KeyValueObjectPointer
import com.ibm.aspen.core.objects.keyvalue.Key
import com.ibm.aspen.core.transaction.KeyValueUpdate.KVRequirement
import com.ibm.aspen.core.transaction.{KeyValueUpdate, TransactionDescription}
import org.apache.logging.log4j.scala.Logging

import scala.concurrent.{ExecutionContext, Future, Promise}
import scala.util.{Failure, Success}


object TieredKeyValueListJoinFA extends Logging {
  val FinalizationActionUUID: UUID = UUID.fromString("fb262a22-8aea-4bbd-8da8-9c3a939c3786")
  
  /** 
   * @param transaction Transaction to add the FinalizationAction to
   * @param mtkvl MutableTieredKeyValueList being modified
   * @param targetTier the tier number into which the new node pointer should be inserted
   * @param left Pointer to the node to the left of the one removed
   * @param removed Pointer to the removed node
   */
  def addFinalizationAction(
      transaction: Transaction, 
      mtkvl: MutableTieredKeyValueList,
      targetTier: Int,
      left: KeyValueListPointer,
      removed: KeyValueListPointer): Unit = {
    
    // TODO: Support removal of the root node. For now we'll skip join finalizers for all left-most nodes. Tree structure and depth is preserved
    //       we just loose the uncached lookup efficiency typically found in smaller trees.  
    if ( removed.minimum != Key.AbsoluteMinimum ) {
      val content = TieredKeyValueListSplitFA.Content(mtkvl.keyOrdering, mtkvl.rootManager.typeUUID, mtkvl.rootManager.serialize(), 
          targetTier, left, removed :: Nil)

      val serializedContent = BaseCodec.encodeTieredKeyValueListSplitFA(content)
      
      transaction.addFinalizationAction(FinalizationActionUUID, serializedContent)
    }
  }
      
  class RemoveFromUpperTier(val system: AspenSystem,
                            val parentTransactionUUID: UUID,
                            val c: TieredKeyValueListSplitFA.Content)(implicit ec: ExecutionContext) extends FinalizationAction {
    
    def remove(mtkvl: MutableTieredKeyValueList): Future[Unit] = system.transact { implicit tx =>
        
      def onSplit(left: KeyValueListPointer, right: List[KeyValueListPointer]): Unit = {}
      
      def onJoin(left: KeyValueListPointer, removed: KeyValueListPointer): Unit = {
        addFinalizationAction(tx, mtkvl, c.targetTier+1, left, removed)
      }
      
      // The removed node will be the only entry in the "inserted" list 
      val removed = c.inserted.head
      
      for {
        
        kvos <- mtkvl.fetchContainingNode(removed.minimum, c.targetTier) 
        
        ovalue = kvos.contents.get(removed.minimum)
        
        if ovalue.isDefined
        
        value = ovalue.get
        
        if removed.pointer == KeyValueObjectPointer(value.value)
        
        allocater <- mtkvl.allocater.tierNodeAllocater(c.targetTier)
        
        nodeSizeLimit = mtkvl.allocater.tierNodeSizeLimit(c.targetTier)
        nodeKVPairLimit = mtkvl.allocater.tierNodeKVPairLimit(c.targetTier)
        inserts = Nil
        deletes = List(removed.minimum)
        requirements = List(KVRequirement(removed.minimum, value.timestamp, KeyValueUpdate.TimestampRequirement.Equals))
        
        _ = tx.ensureHappensAfter(value.timestamp)
        
        _ <- system.readObject(kvos.pointer)

        _=tx.note(s"TieredKeyValueListJoinFA($parentTransactionUUID) - removing pointer to ${removed.pointer.uuid} from upper tier ${c.targetTier}")

        _ <- KeyValueList.prepreUpdateTransaction(kvos, nodeSizeLimit, nodeKVPairLimit, inserts, deletes, requirements, c.keyOrdering, system, allocater, onSplit, onJoin)
        
        _ <- tx.commit()
      } yield ()
    }
    
    val complete: Future[Unit] = system.retryStrategy.retryUntilSuccessful {
      val p = Promise[Unit]()
      
      MutableTieredKeyValueList.load(system, c.rootManagerType, c.serializedRootManager) onComplete {
        case Failure(err) =>
          logger.error(s"Failed to load MutableTieredKeyValueList for Join FinalizationAction. Error: $err")
          p.success(()) // Nothing we can do to recover. Hopefully the tree has been deleted
          
        case Success(mtkvl) => 
          p.completeWith(remove(mtkvl))
      }
      
      p.future
    }
  }
}

class TieredKeyValueListJoinFA extends FinalizationActionHandler {
  
  import TieredKeyValueListJoinFA._
  
  val typeUUID: UUID = FinalizationActionUUID
  
  def createAction(
      system: AspenSystem,
      txd: TransactionDescription,
      serializedActionData: Array[Byte])(implicit ec: ExecutionContext): FinalizationAction = {
    new RemoveFromUpperTier(system, txd.transactionUUID, BaseCodec.decodeTieredKeyValueListSplitFA(serializedActionData) )
  }
}