package com.ibm.aspen.base.tieredlist

import com.ibm.aspen.base.{AspenSystem, Transaction}
import com.ibm.aspen.core.objects.{KeyValueObjectPointer, KeyValueObjectState}
import com.ibm.aspen.core.objects.keyvalue.Key
import com.ibm.aspen.core.transaction.KeyValueUpdate

import scala.concurrent.{ExecutionContext, Future}

class MutableTieredKeyValueListNode(
    val system: AspenSystem,
    val mtkvl: MutableTieredKeyValueList,
    val kvos: KeyValueObjectState) {
  
  private def reader = mtkvl.getObjectReaderForTier(0)
  
  /** Note that this future will fail if the right node node has been deleted */
  def fetchRight()(implicit ec: ExecutionContext): Future[Option[MutableTieredKeyValueListNode]] = kvos.right match {
    case None => Future.successful(None)
    case Some(ptr) => reader.readObject(KeyValueObjectPointer(ptr.content)).map(kvos => Some(new MutableTieredKeyValueListNode(system, mtkvl, kvos)))
  }
    
  /** Note that this future will fail if the node has been deleted */
  def refresh()(implicit ec: ExecutionContext): Future[MutableTieredKeyValueListNode] = {
    reader.readObject(kvos.pointer).map(kvos => new MutableTieredKeyValueListNode(system, mtkvl, kvos))
  }
    
  def prepreUpdateTransaction(
    inserts: List[(Key, Array[Byte])],
    deletes: List[Key],
    requirements: List[KeyValueUpdate.KVRequirement])(implicit tx: Transaction, ec: ExecutionContext): Future[Future[MutableTieredKeyValueListNode]] = {
    
    def onSplit(left: KeyValueListPointer, right: List[KeyValueListPointer]): Unit = {
      tx.note(s"Splitting TKVL. Left node ${left.pointer.uuid}, Right nodes ${right.map(_.pointer.uuid)}")
      TieredKeyValueListSplitFA.addFinalizationAction(tx, mtkvl, 1, left, right)
    }
    
    def onJoin(left: KeyValueListPointer, removed: KeyValueListPointer): Unit = {
      tx.note(s"Joining TKVL. Left node ${left.pointer.uuid}. Removed node ${removed.pointer.uuid}")
      TieredKeyValueListJoinFA.addFinalizationAction(tx, mtkvl, 1, left, removed)
    }
    
    val fallocater = mtkvl.allocater.tierNodeAllocater(0)
    val pairLimit = mtkvl.allocater.tierNodeKVPairLimit(0)
    val sizeLimit = mtkvl.allocater.tierNodeSizeLimit(0)

    for {
      allocater <- fallocater
      fupdatedKvos <- KeyValueList.prepreUpdateTransaction(kvos, sizeLimit, pairLimit, inserts, deletes, requirements, 
                     mtkvl.keyOrdering, reader, allocater, onSplit, onJoin)
    } yield {
      tx.result.flatMap(_ => fupdatedKvos.map(updatedKvos => new MutableTieredKeyValueListNode(system, mtkvl, updatedKvos)))
    }
  }
}