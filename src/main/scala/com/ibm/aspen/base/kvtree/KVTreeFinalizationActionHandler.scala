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
import com.ibm.aspen.core.network.{Codec => NetworkCodec}

class KVTreeFinalizationActionHandler(
    val treeFactory: KVTreeFactory,
    val retryStrategy: RetryStrategy,
    val system: AspenSystem) extends FinalizationActionHandler {
  
  import KVTreeFinalizationActionHandler._
  
  val supportedUUIDs: Set[UUID] = KVTreeFinalizationActionHandler.supportedUUIDs
  
  class InsertIntoUpperTierFA(
      val treeDefinitionPointer: ObjectPointer, 
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
            val oldRootPointer = KVListNodePointer(tree.rootTier.rootObjectPointer, new Array[Byte](0))
            tree.createNextTier(oldRootPointer :: nodePointer :: Nil).map(x => ())
          } else {
            tree.fetchContainingNode(nodePointer.minimum, targetTier) flatMap {
              t =>
                implicit val tx = system.newTransaction()
                val encPtr = NetworkCodec.objectPointerToByteArray(nodePointer.objectPointer)
                t._2.update((nodePointer.minimum, encPtr)::Nil, Nil, tree.onListNodeSplit(targetTier))
                tx.commit()
            }
          }
      } 
    }
  
    def completionDetected(): Unit = ()
  }
  
  def createAction(
      finalizationActionUUID: UUID, 
      serializedActionData: Array[Byte]): Option[FinalizationAction] = finalizationActionUUID match {
    case InsertIntoUpperTierUUID => 
      val c = KVTreeCodec.decodeInsertIntoUpperTierFinalizationAction(serializedActionData)
      
      Some(new InsertIntoUpperTierFA(c.treeDefinitionPointer, c.targetTier, c.nodePointer))
      
    case _ => None
  }
}

object KVTreeFinalizationActionHandler {
  val InsertIntoUpperTierUUID = UUID.fromString("3a424707-cb50-4fcd-a575-595c3bbb8c77")
  
  val supportedUUIDs: Set[UUID] = Set(InsertIntoUpperTierUUID)
  
  def insertIntoUpperTier(transaction: Transaction, tree: KVTree, targetTier: Int, nodePointer: KVListNodePointer): Unit = {
    val serializedContent = KVTreeCodec.encodeInsertIntoUpperTierFinalizationAction(tree, targetTier, nodePointer)
    transaction.addFinalizationAction(InsertIntoUpperTierUUID, serializedContent)
  }
}