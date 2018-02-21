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
        val (containerKvos, root) = t
        
        if (root.topTier < c.targetTier) {
          // Create new tier
          val iLeft  = Insert(c.left.minimum.bytes, c.left.pointer.toArray, tx.timestamp())
          val iRight = Insert(c.right.minimum.bytes, c.right.pointer.toArray, tx.timestamp())
          
          for {
            allocater <- system.getObjectAllocater(root.getTierNodeAllocaterUUID(c.targetTier))
            
            newRootPointer <- allocater.allocateKeyValueObject(
                allocatingObject = containerKvos.pointer, 
                allocatingObjectRevision = containerKvos.revision, 
                initialContent = List(iLeft, iRight))
                
            commitReady <- lst.prepreUpdateRootTransaction(c.targetTier, newRootPointer)
            
            done <- tx.commit()
            
          } yield ()
        } else {
          
          def onSplit(left: KeyValueListPointer, right: KeyValueListPointer): Unit = {
            addFinalizationAction(tx, c.treeIdentifier, c.treeContainer, c.keyOrdering, c.targetTier+1, left, right)
          }
          
          def onJoin(removedPointer: KeyValueObjectPointer): Unit = {}
          
          for {
            kvos <- lst.fetchContainingNode(c.right.minimum, c.targetTier) 
            
            allocater <- system.getObjectAllocater(root.getTierNodeAllocaterUUID(c.targetTier))
            
            nodeSizeLimit = root.getTierNodeSize(c.targetTier)
            inserts = List((c.right.minimum, c.right.pointer.toArray))
            deletes = Nil
            requirements = Nil
            
            test <- system.readObject(kvos.pointer)

            ready <- KeyValueList.prepreUpdateTransaction(kvos, nodeSizeLimit, inserts, deletes, requirements, c.keyOrdering, system, allocater, onSplit, onJoin)
            
            done <- tx.commit()

          } yield ()
        }
      }
    }    
  }
  
}