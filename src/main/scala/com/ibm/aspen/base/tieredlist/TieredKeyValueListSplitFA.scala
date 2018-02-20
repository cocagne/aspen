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
      targetTier: Int, newNode: KeyValueListPointer): Unit = {
    
    val serializedContent = BaseCodec.encodeTieredKeyValueListSplitFA(treeIdentifier, treeContainer, keyOrdering, targetTier, newNode)
        
    transaction.addFinalizationAction(FinalizationActionUUID, serializedContent)
  }
  
  case class Content(
      treeIdentifier: Key, 
      treeContainer: Either[KeyValueObjectPointer, TieredKeyValueList.Root], 
      targetTier: Int, 
      newNode: KeyValueListPointer,
      keyOrdering: KeyOrdering) 
}

class TieredKeyValueListSplitFA(
    val retryStrategy: RetryStrategy,
    val system: AspenSystem) extends FinalizationActionHandler {
  
  import TieredKeyValueListSplitFA._
  
  val finalizationActionUUID: UUID = FinalizationActionUUID
  
  def createAction(serializedActionData: Array[Byte]): FinalizationAction = {
    new InsertIntoUpperTier( BaseCodec.decodeTieredKeyValueListSplitFA(serializedActionData) )
  }
  
  class InsertIntoUpperTier(val c: Content) extends FinalizationAction {
    
    def completionDetected(): Unit = ()
    
    def execute()(implicit ec: ExecutionContext): Future[Unit] = retryStrategy.retryUntilSuccessful {
      val lst = new SimpleMutableTieredKeyValueList(system, c.treeContainer, c.treeIdentifier, c.keyOrdering)
      
      implicit val tx = system.newTransaction()
      
      lst.refreshRoot() flatMap { t =>
        val root = t._2
        
        if (root.topTier < c.targetTier) {
          
          lst.prepreUpdateRootTransaction(c.targetTier, c.newNode.pointer).flatMap(_ => tx.commit())
          
        } else {
          
          def onSplit(newMinimum: Key, newPointer: KeyValueObjectPointer): Unit = {
            addFinalizationAction(tx, c.treeIdentifier, c.treeContainer, c.keyOrdering, c.targetTier+1, KeyValueListPointer(newMinimum, newPointer))
          }
          
          def onJoin(removedPointer: KeyValueObjectPointer): Unit = {}
          
          for {
            kvos <- lst.fetchContainingNode(c.newNode.minimum, c.targetTier) 
            allocater <- system.getObjectAllocater(root.getTierNodeAllocaterUUID(c.targetTier))
            
            nodeSizeLimit = root.getTierNodeSize(c.targetTier)
            inserts = List((c.newNode.minimum, c.newNode.pointer.toArray))
            deletes = Nil
            requirements = Nil
            
            ready <- KeyValueList.prepreUpdateTransaction(kvos, nodeSizeLimit, inserts, deletes, requirements, c.keyOrdering, system, allocater, onSplit, onJoin)
            done <- tx.commit()
            
          } yield ()
        }
      } 
    }
  }
  
}