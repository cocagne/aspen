package com.ibm.aspen.base.kvtree

import com.ibm.aspen.base.Transaction
import com.ibm.aspen.base.kvlist.KVListNodePointer
import com.ibm.aspen.base.FinalizationActionHandler
import java.util.UUID
import com.ibm.aspen.base.FinalizationAction
import scala.concurrent.ExecutionContext
import scala.concurrent.Future

object KVTreeFinalizationActions extends FinalizationActionHandler {
  
  val InsertIntoUpperTierUUID = UUID.fromString("3a424707-cb50-4fcd-a575-595c3bbb8c77")  

  val supportedUUIDs: Set[UUID] = Set(InsertIntoUpperTierUUID)
  
  class InsertIntoUpperTierFA(val tree: KVTree, val targetTier: Int, nodePointer:KVListNodePointer) extends FinalizationAction {
    def execute()(implicit ec: ExecutionContext): Future[Unit] = Future.successful(())
  
    def completionDetected(): Unit = ()
  }
  
  def createAction(finalizationActionUUID: UUID, serializedActionData: Array[Byte]): Option[FinalizationAction] = finalizationActionUUID match {
    case InsertIntoUpperTierUUID => 
      val c = KVTreeCodec.decodeInsertIntoUpperTierFinalizationAction(serializedActionData)
      //Some(new InsertIntoUpperTierFA())
      None
  }
  
  def insertIntoUpperTier(transaction: Transaction, tree: KVTree, targetTier: Int, nodePointer: KVListNodePointer): Unit = {
    val serializedContent = KVTreeCodec.encodeInsertIntoUpperTierFinalizationAction(tree, targetTier, nodePointer)
    transaction.addFinalizationAction(InsertIntoUpperTierUUID, serializedContent)
  }
}