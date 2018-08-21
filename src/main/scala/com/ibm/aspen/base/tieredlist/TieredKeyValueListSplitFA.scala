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
      right: List[KeyValueListPointer]): Unit = {
    
    val serializedContent = BaseCodec.encodeTieredKeyValueListSplitFA(treeIdentifier, treeContainer, keyOrdering, targetTier, left, right)
        
    transaction.addFinalizationAction(FinalizationActionUUID, serializedContent)
  }
  
  case class Content(
      treeIdentifier: Key, 
      treeContainer: Either[KeyValueObjectPointer, TieredKeyValueList.Root], 
      targetTier: Int, 
      left: KeyValueListPointer, 
      inserted: List[KeyValueListPointer],
      keyOrdering: KeyOrdering)
      
  class InsertIntoUpperTier(val system: AspenSystem, val c: Content)(implicit ec: ExecutionContext) extends FinalizationAction {
    println(s"BEGINNING INSERT INTO UPPER TIER FA")
    val complete = system.retryStrategy.retryUntilSuccessful {
      val lst = new SimpleMutableTieredKeyValueList(system, c.treeContainer, c.treeIdentifier, c.keyOrdering)
      
      def createNewTier(containerKvos: KeyValueObjectState, root: TieredKeyValueList.Root, inserted: List[KeyValueListPointer]): Future[Unit] = system.transact { implicit tx =>
        println(s"  CREATING NEW TIER")
        val ops  = Insert(c.left.minimum, c.left.pointer.toArray) :: inserted.map(p => Insert(p.minimum, p.pointer.toArray))
        
        for {
          allocater <- system.getObjectAllocater(root.getTierNodeAllocaterUUID(c.targetTier))
          
          newRootPointer <- allocater.allocateKeyValueObject(
              allocatingObject = containerKvos.pointer, 
              allocatingObjectRevision = containerKvos.revision, 
              initialContent = ops)
              
          commitReady <- lst.prepreUpdateRootTransaction(c.targetTier, newRootPointer)
        } yield ()
      }
      
      def insertIntoExistingTier(root: TieredKeyValueList.Root, right: KeyValueListPointer): Future[Unit] = system.transact { implicit tx =>
        
        def onSplit(left: KeyValueListPointer, inserted: List[KeyValueListPointer]): Unit = {
          addFinalizationAction(tx, c.treeIdentifier, c.treeContainer, c.keyOrdering, c.targetTier+1, left, inserted)
        }
        
        def onJoin(left: KeyValueListPointer, removed: KeyValueListPointer): Unit = {}
        
        for {
          kvos <- lst.fetchContainingNode(right.minimum, c.targetTier) 
          
          allocater <- system.getObjectAllocater(root.getTierNodeAllocaterUUID(c.targetTier))
          
          nodeSizeLimit = root.getTierNodeSize(c.targetTier)
          nodeKVPairLimit = root.getTierNodeKVPairLimit(c.targetTier)
          inserts = List((right.minimum, right.pointer.toArray))
          deletes = Nil
          requirements = Nil

          ready <- KeyValueList.prepreUpdateTransaction(kvos, nodeSizeLimit, nodeKVPairLimit, inserts, deletes, requirements, c.keyOrdering, system, allocater, onSplit, onJoin)

        } yield ()
      }
      
      val p = Promise[Unit]()
      
      lst.refreshRoot() onComplete {
        case Success((containerKvos, root)) =>
          val fadd = if (root.topTier < c.targetTier) 
            createNewTier(containerKvos, root, c.inserted) 
          else 
            Future.sequence(c.inserted.map(kvlp => insertIntoExistingTier(root, kvlp))).map(_ => ())
          
          p.completeWith(fadd)
        
        case Failure(err) =>
          println(s"REFRESH ROOT ERR $err")
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
      serializedActionData: Array[Byte])(implicit ec: ExecutionContext): FinalizationAction = {
    new InsertIntoUpperTier(system, BaseCodec.decodeTieredKeyValueListSplitFA(serializedActionData))
  }
}