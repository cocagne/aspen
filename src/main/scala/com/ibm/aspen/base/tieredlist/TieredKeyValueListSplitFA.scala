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
import com.ibm.aspen.core.DataBuffer
import org.apache.logging.log4j.scala.Logging

object TieredKeyValueListSplitFA extends Logging {
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
      mtkvl: MutableTieredKeyValueList,
      targetTier: Int, 
      left: KeyValueListPointer, 
      right: List[KeyValueListPointer]): Unit = {
    
    val content = Content(mtkvl.keyOrdering, mtkvl.rootManager.typeUUID, mtkvl.rootManager.serialize(), targetTier, left, right)
    
    val serializedContent = BaseCodec.encodeTieredKeyValueListSplitFA(content)
        
    transaction.addFinalizationAction(FinalizationActionUUID, serializedContent)
  }
  
  case class Content(
      keyOrdering: KeyOrdering,
      rootManagerType: UUID,
      serializedRootManager: DataBuffer,
      targetTier: Int,
      left: KeyValueListPointer, 
      inserted: List[KeyValueListPointer])
      
  class InsertIntoUpperTier(val system: AspenSystem, val c: Content)(implicit ec: ExecutionContext) extends FinalizationAction {

    def createNewTier(
        mtkvl: MutableTieredKeyValueList,
        newRootTier: Int,
        inserted: List[KeyValueListPointer]): Future[Unit] = system.transact { implicit tx =>
      mtkvl.rootManager.prepareRootUpdate(newRootTier, mtkvl.allocater, inserted)
    }

    
    def insertIntoExistingTier(
        mtkvl: MutableTieredKeyValueList,
        toInsert: List[KeyValueListPointer]) : Future[Unit] = if (toInsert.isEmpty) Future.successful(()) else {

      implicit val tx = system.newTransaction()
      
      def onSplit(left: KeyValueListPointer, inserted: List[KeyValueListPointer]): Unit = {
        addFinalizationAction(tx, mtkvl, c.targetTier+1, left, inserted)
      }
      
      def onJoin(left: KeyValueListPointer, removed: KeyValueListPointer): Unit = {}
      
      val f = for {
        kvos <- mtkvl.fetchContainingNode(toInsert.head.minimum, c.targetTier) 
        
        allocater <- mtkvl.allocater.tierNodeAllocater(c.targetTier)
        
        nodeSizeLimit = mtkvl.allocater.tierNodeSizeLimit(c.targetTier)
        nodeKVPairLimit = mtkvl.allocater.tierNodeKVPairLimit(c.targetTier)
        
        (ins, remaining) = toInsert.partition(lp => kvos.keyInRange(lp.minimum, c.keyOrdering))
        
        inserts = ins.map(lp => (lp.minimum, lp.pointer.toArray))
        deletes = Nil
        requirements = Nil

        _=tx.note(s"TieredKeyValueListSplitFA - Adding pointer(s) to ${ins.map(_.pointer.uuid)} to tier ${c.targetTier}")

        ready <- KeyValueList.prepreUpdateTransaction(kvos, nodeSizeLimit, nodeKVPairLimit, inserts, deletes, requirements, c.keyOrdering, system, allocater, onSplit, onJoin)
        
        _ <- tx.commit()

      } yield remaining
      
      f.flatMap(remaining => insertIntoExistingTier(mtkvl, remaining))
    }
    
    val complete = system.retryStrategy.retryUntilSuccessful {
      val p = Promise[Unit]()
      
      MutableTieredKeyValueList.load(system, c.rootManagerType, c.serializedRootManager) onComplete {
        case Failure(err) =>
          logger.error(s"Failed to load MutableTieredKeyValueList for Split FinalizationAction. Error: $err")
          p.success(()) // Nothing we can do to recover. Hopefully the tree has been deleted
          
        case Success(mtkvl) =>
          val fadd = if (c.targetTier > mtkvl.rootManager.root.topTier)
            createNewTier(mtkvl, c.targetTier, c.inserted)
          else
            insertIntoExistingTier(mtkvl, c.inserted)
            //Future.sequence(c.inserted.map(kvlp => insertIntoExistingTier(mtkvl, kvlp))).map(_ => ())
          p.completeWith(fadd)
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