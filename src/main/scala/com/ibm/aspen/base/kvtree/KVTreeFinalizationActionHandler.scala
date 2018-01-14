package com.ibm.aspen.base.kvtree

import com.ibm.aspen.base.Transaction
import com.ibm.aspen.base.kvlist.KVListNodePointer
import com.ibm.aspen.base.FinalizationActionHandler
import java.util.UUID
import com.ibm.aspen.base.FinalizationAction
import scala.concurrent.ExecutionContext
import scala.concurrent.Future
import com.ibm.aspen.core.objects.ObjectPointer
import com.ibm.aspen.base.RetryStrategy
import com.ibm.aspen.base.AspenSystem
import com.ibm.aspen.core.network.NetworkCodec
import com.ibm.aspen.core.objects.DataObjectPointer

object KVTreeFinalizationActionHandler {
  val InsertIntoUpperTierUUID = UUID.fromString("3a424707-cb50-4fcd-a575-595c3bbb8c77")
  
  def insertIntoUpperTier(transaction: Transaction, tree: KVTree, targetTier: Int, nodePointer: KVListNodePointer): Unit = {
    val serializedContent = KVTreeCodec.encodeInsertIntoUpperTierFinalizationAction(tree, targetTier, nodePointer)
    transaction.addFinalizationAction(InsertIntoUpperTierUUID, serializedContent)
  }
}

class KVTreeFinalizationActionHandler(
    val treeFactory: KVTreeFactory,
    val retryStrategy: RetryStrategy,
    val system: AspenSystem) extends FinalizationActionHandler {
  
  import KVTreeFinalizationActionHandler._
  
  val finalizationActionUUID: UUID = InsertIntoUpperTierUUID
  
  
  class InsertIntoUpperTierFA(
      val treeDefinitionPointer: DataObjectPointer, 
      val targetTier: Int, 
      val nodePointer:KVListNodePointer) extends FinalizationAction {
    
    def execute()(implicit ec: ExecutionContext): Future[Unit] = retryStrategy.retryUntilSuccessful {
      //
      // TODO: createTree will forever fail if the tree description object is deleted (old Tx could be recovered after tree is deleted)
      //       detect this condition and return success to retryUntilSuccessful
      //
      treeFactory.createTree(treeDefinitionPointer) flatMap { 
        tree =>
          if (targetTier > tree.numTiers) {
            tree.refresh()
            return Future.failed(new Exception("Tree is out of date. Refreshing tree and relying on retry mechanism to re-execute this method"))
          }
          
          if (targetTier == tree.numTiers) {
            // Create the next tier up
            val oldRootPointer = KVListNodePointer(tree.rootTier.rootObjectPointer, new Array[Byte](0))
            tree.createNextTier(oldRootPointer :: nodePointer :: Nil).map(x => ())
          } else {
            // Insert into an existing tier
            implicit val tx = system.newTransaction()
            
            for {
              (_, node) <- tree.fetchContainingNode(nodePointer.minimum, targetTier)
              encPtr = NetworkCodec.objectPointerToByteArray(nodePointer.objectPointer)
              // Update returns a Future to a Future. The outer future is to when the Transaction is ready to commit.
              // On node splits, ensure the pointer to the new node goes into the tier above this one
              commitReady <- node.update((nodePointer.minimum, encPtr)::Nil, Nil, tree.onListNodeSplit(targetTier+1))
              complete <- tx.commit() 
            } yield ()
          }
      } 
    }
  
    def completionDetected(): Unit = ()
  }
  
  def createAction(serializedActionData: Array[Byte]): FinalizationAction = {
    val c = KVTreeCodec.decodeInsertIntoUpperTierFinalizationAction(serializedActionData)
      
    new InsertIntoUpperTierFA(c.treeDefinitionPointer, c.targetTier, c.nodePointer)
  }
}

