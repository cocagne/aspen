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
import com.ibm.aspen.core.objects.keyvalue.Insert
import com.ibm.aspen.core.objects.KeyValueObjectState
import scala.concurrent.Promise
import com.ibm.aspen.core.data_store.DataStoreID
import com.ibm.aspen.core.transaction.TransactionDescription

object TieredKeyValueListSplitFA {
  val FinalizationActionUUID = UUID.fromString("09d1c4a2-22a7-48b0-85f4-726afea02f2a")
  
  /** 
   * @param transaction Transaction to add the FinalizationAction to
   * @param treeIdentifier Unique identifer for the tree root key-value pair
   * @param treeContainer Either a stand-alone key-value object that contains the tree root or a tiered list that contains the key
   * @param keyOrdering Defines key comparisons for sorting
   * @param targetTier the tier number into which the new node pointer should be inserted
   * @param newNode the new node to be inserted into the tier
   */
  def addFinalizationAction(
      transaction: Transaction, 
      treeIdentifier: Key, 
      treeContainer: Either[KeyValueObjectPointer, TieredKeyValueList.Root],
      keyOrdering: KeyOrdering,
      targetTier: Int, 
      left: KeyValueListPointer, 
      right: KeyValueListPointer): Unit = {
    
    val serializedContent = BaseCodec.encodeTieredKeyValueListSplitFA(treeIdentifier, treeContainer, keyOrdering, targetTier, left, right)
        
    transaction.addFinalizationAction(FinalizationActionUUID, serializedContent)
  }
  
  case class Content(
      treeIdentifier: Key, 
      treeContainer: Either[KeyValueObjectPointer, TieredKeyValueList.Root], 
      targetTier: Int, 
      left: KeyValueListPointer, 
      right: KeyValueListPointer,
      keyOrdering: KeyOrdering)
      
  class InsertIntoUpperTier(val system: AspenSystem, val c: Content) extends FinalizationAction {
    
    def execute()(implicit ec: ExecutionContext): Future[Unit] = system.retryStrategy.retryUntilSuccessful {
      val lst = new SimpleMutableTieredKeyValueList(system, c.treeContainer, c.treeIdentifier, c.keyOrdering)
      
      def createNewTier(containerKvos: KeyValueObjectState, root: TieredKeyValueList.Root): Future[Unit] = system.transact { implicit tx =>
        
        val iLeft  = Insert(c.left.minimum.bytes, c.left.pointer.toArray, tx.timestamp())
        val iRight = Insert(c.right.minimum.bytes, c.right.pointer.toArray, tx.timestamp())
        
        for {
          allocater <- system.getObjectAllocater(root.getTierNodeAllocaterUUID(c.targetTier))
          
          newRootPointer <- allocater.allocateKeyValueObject(
              allocatingObject = containerKvos.pointer, 
              allocatingObjectRevision = containerKvos.revision, 
              initialContent = List(iLeft, iRight))
              
          commitReady <- lst.prepreUpdateRootTransaction(c.targetTier, newRootPointer)
        } yield ()
      }
      
      def insertIntoExistingTier(root: TieredKeyValueList.Root): Future[Unit] = system.transact { implicit tx =>
        
        def onSplit(left: KeyValueListPointer, right: KeyValueListPointer): Unit = {
          addFinalizationAction(tx, c.treeIdentifier, c.treeContainer, c.keyOrdering, c.targetTier+1, left, right)
        }
        
        def onJoin(left: KeyValueListPointer, removed: KeyValueListPointer): Unit = {}
        
        for {
          kvos <- lst.fetchContainingNode(c.right.minimum, c.targetTier) 
          
          allocater <- system.getObjectAllocater(root.getTierNodeAllocaterUUID(c.targetTier))
          
          nodeSizeLimit = root.getTierNodeSize(c.targetTier)
          inserts = List((c.right.minimum, c.right.pointer.toArray))
          deletes = Nil
          requirements = Nil

          ready <- KeyValueList.prepreUpdateTransaction(kvos, nodeSizeLimit, inserts, deletes, requirements, c.keyOrdering, system, allocater, onSplit, onJoin)

        } yield ()
      }
      
      val p = Promise[Unit]()
      
      lst.refreshRoot() onComplete {
        case Success((containerKvos, root)) =>
          val fadd = if (root.topTier < c.targetTier) createNewTier(containerKvos, root) else insertIntoExistingTier(root)
          
          p.completeWith(fadd)
        
        case Failure(_) =>
          // Failures here must be either due to the containing object being corrupt/deleted or the root key
          // being deleted. In either case there's nothing we can do to recover. Declare success.
          p.success(())
      }
      
      p.future
    }
  }
}

class TieredKeyValueListSplitFA extends FinalizationActionHandler {
  
  import TieredKeyValueListSplitFA._
  
  val typeUUID: UUID = FinalizationActionUUID
  
  def createAction(
      system: AspenSystem,
      txd: TransactionDescription,
      serializedActionData: Array[Byte], 
      successfullyUpdatedPeers: Set[DataStoreID]): FinalizationAction = {
    new InsertIntoUpperTier(system, BaseCodec.decodeTieredKeyValueListSplitFA(serializedActionData))
  }
}