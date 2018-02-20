package com.ibm.aspen.base.tieredlist

import com.ibm.aspen.core.objects.KeyValueObjectPointer
import com.ibm.aspen.base.Transaction
import scala.concurrent.ExecutionContext
import scala.concurrent.Future
import com.ibm.aspen.core.objects.KeyValueObjectState
import com.ibm.aspen.core.objects.keyvalue.Key
import com.ibm.aspen.core.transaction.KeyValueUpdate
import com.ibm.aspen.base.ObjectAllocater

trait MutableTieredKeyValueList extends TieredKeyValueList {
  
  val nodeSizeLimit: Int
  
  protected def getObjectAllocaterForTier(tier: Int): ObjectAllocater
  
  /** Future completes when the transactions is ready to commit */
  protected[tieredlist] def prepreUpdateRootTransaction(newRootPointer: KeyValueObjectPointer)(implicit tx: Transaction, ec: ExecutionContext): Future[Unit]
  
  
  protected[tieredlist] def refreshRootPointer(): Future[(Int, KeyValueObjectPointer)]
  
  
  protected def onSplit(tier: Int)(newPointer: KeyValueObjectPointer): Unit = {
    
  }
  
  protected def onJoin(tier: Int)(newPointer: KeyValueObjectPointer): Unit = {
    
  }
  
  class MutableNode(val kvos: KeyValueObjectState) {
    def prepreUpdateTransaction(
      inserts: List[(Key, Array[Byte])],
      deletes: List[Key],
      requirements: List[KeyValueUpdate.KVRequirement])(implicit tx: Transaction, ec: ExecutionContext): Future[MutableNode] = {
      
      val reader = getObjectReaderForTier(0)
      val allocater = getObjectAllocaterForTier(0)
      
      KeyValueList.prepreUpdateTransaction(kvos, nodeSizeLimit, inserts, deletes, requirements, 
          keyOrdering, reader, allocater, onSplit(0), onJoin(0)).map(new MutableNode(_))
    }
  }
  
  def fetchMutableNode(key: Key)(implicit ec: ExecutionContext): Future[MutableNode] = fetchContainingNode(key, 0).map(new MutableNode(_))
}