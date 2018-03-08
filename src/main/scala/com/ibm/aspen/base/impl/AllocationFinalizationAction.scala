package com.ibm.aspen.base.impl

import java.util.UUID
import com.ibm.aspen.base.Transaction
import com.ibm.aspen.core.objects.ObjectPointer
import com.ibm.aspen.core.network.NetworkCodec
import java.nio.ByteBuffer
import com.ibm.aspen.base.FinalizationActionHandler
import com.ibm.aspen.base.FinalizationAction
import scala.concurrent.ExecutionContext
import scala.concurrent.Future
import com.ibm.aspen.base.RetryStrategy
import com.ibm.aspen.base.AspenSystem
import com.ibm.aspen.base.kvtree.KVTreeSimpleFactory
import com.ibm.aspen.base.kvtree.KVTreeNodeCache
import com.ibm.aspen.base.kvtree.KVTree
import com.ibm.aspen.core.data_store.DataStoreID
import com.ibm.aspen.core.objects.DataObjectPointer
import com.ibm.aspen.core.objects.KeyValueObjectPointer

object AllocationFinalizationAction {
  val AddToAllocationTreeUUID = UUID.fromString("909ce37d-a138-44a5-9498-f56095827cdf")
  
  case class FAContent(storagePoolDefinitionPointer:KeyValueObjectPointer, newNodePointer:ObjectPointer)
  
  def addToAllocationTree(transaction: Transaction, storagePoolDefinitionPointer:KeyValueObjectPointer, newNodePointer:ObjectPointer): Unit = {
    val serializedContent = BaseCodec.encode(FAContent(storagePoolDefinitionPointer, newNodePointer))
    transaction.addFinalizationAction(AddToAllocationTreeUUID, serializedContent)
    
    val notifyStores = newNodePointer.storePointers.map(sp => DataStoreID(newNodePointer.poolUUID, sp.poolIndex)).toSet
    transaction.addNotifyOnResolution(notifyStores)
  }
}

class AllocationFinalizationAction(
    val retryStrategy: RetryStrategy,
    val system: AspenSystem) extends FinalizationActionHandler {
  
  import AllocationFinalizationAction._
  
  val typeUUID: UUID = AddToAllocationTreeUUID
  
  class AddToAllocationTree(
      val storagePoolDefinitionPointer:KeyValueObjectPointer, 
      val newNodePointer:ObjectPointer) extends FinalizationAction {
    
    def execute()(implicit ec: ExecutionContext): Future[Unit] = retryStrategy.retryUntilSuccessful {
      //
      // TODO: getStoragePool will forever fail if the pool description object is deleted (old Tx could be recovered after pool is deleted)
      //       detect this condition and return success to retryUntilSuccessful
      //
      implicit val tx = system.newTransaction()
      
      for {
        pool <- system.getStoragePool(storagePoolDefinitionPointer)
        tree <- pool.getAllocationTree(retryStrategy)
        commitReady <- tree.put(newNodePointer.uuid, newNodePointer.toArray)
        result <- tx.commit()
      } yield ()
    }
  
    def completionDetected(): Unit = ()
  }
  
  override def createAction(serializedActionData: Array[Byte]): FinalizationAction = {

    val fa = BaseCodec.decodeFinalizationActionContent(serializedActionData)
    
    new AddToAllocationTree(fa.storagePoolDefinitionPointer, fa.newNodePointer)      
  }
}

