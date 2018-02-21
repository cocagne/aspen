package com.ibm.aspen.base.tieredlist

import com.ibm.aspen.core.objects.KeyValueObjectPointer
import com.ibm.aspen.base.Transaction
import scala.concurrent.ExecutionContext
import scala.concurrent.Future
import com.ibm.aspen.core.objects.KeyValueObjectState
import com.ibm.aspen.core.objects.keyvalue.Key
import com.ibm.aspen.core.transaction.KeyValueUpdate
import com.ibm.aspen.base.ObjectAllocater
import com.ibm.aspen.base.AspenSystem

trait MutableTieredKeyValueList extends TieredKeyValueList {
  
  val system: AspenSystem
  
  protected val treeIdentifier: Key
  protected val treeContainer: Either[KeyValueObjectPointer, TieredKeyValueList.Root]
  
  protected def getObjectAllocaterForTier(tier: Int)(implicit ec: ExecutionContext): Future[ObjectAllocater]
  
  /** Future completes when the transactions is ready to commit */
  protected[tieredlist] def prepreUpdateRootTransaction(
      newTier: Int,
      newRootPointer: KeyValueObjectPointer)(implicit tx: Transaction, ec: ExecutionContext): Future[Unit]
  
  
  protected[tieredlist] def refreshRoot()(implicit ec: ExecutionContext): Future[(KeyValueObjectState, TieredKeyValueList.Root)]
  
  
  class MutableNode(val kvos: KeyValueObjectState) {
    def prepreUpdateTransaction(
      inserts: List[(Key, Array[Byte])],
      deletes: List[Key],
      requirements: List[KeyValueUpdate.KVRequirement])(implicit tx: Transaction, ec: ExecutionContext): Future[MutableNode] = {
      
      val reader = getObjectReaderForTier(0) 
      
      def onSplit(left: KeyValueListPointer, right: KeyValueListPointer): Unit = {
        TieredKeyValueListSplitFA.addFinalizationAction(tx, treeIdentifier, treeContainer, keyOrdering, 1, left, right)
      }
      
      def onJoin(newPointer: KeyValueObjectPointer): Unit = {
        
      }
      
      val fallocater = getObjectAllocaterForTier(0)
      
      for {
        allocater <- fallocater
        root <- rootPointer()
        updatedKvos <- KeyValueList.prepreUpdateTransaction(kvos, root.getTierNodeSize(0), inserts, deletes, requirements, 
                       keyOrdering, reader, allocater, onSplit, onJoin)
      } yield {
        new MutableNode(updatedKvos)
      }
    }
  }
  
  def fetchMutableNode(key: Key)(implicit ec: ExecutionContext): Future[MutableNode] = fetchContainingNode(key, 0).map(new MutableNode(_))
}