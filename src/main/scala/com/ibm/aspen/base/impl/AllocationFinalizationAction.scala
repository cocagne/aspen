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
import com.ibm.aspen.core.data_store.DataStoreID
import com.ibm.aspen.core.objects.DataObjectPointer
import com.ibm.aspen.core.objects.KeyValueObjectPointer
import com.ibm.aspen.core.transaction.TransactionDescription

object AllocationFinalizationAction {
  val AddToAllocationTreeUUID = UUID.fromString("909ce37d-a138-44a5-9498-f56095827cdf")
  
  case class FAContent(storagePoolDefinitionPointer:KeyValueObjectPointer, newNodePointer:ObjectPointer)
  
  def addToAllocationTree(transaction: Transaction, storagePoolDefinitionPointer:KeyValueObjectPointer, newNodePointer:ObjectPointer): Unit = {
    val serializedContent = BaseCodec.encode(FAContent(storagePoolDefinitionPointer, newNodePointer))
    transaction.addFinalizationAction(AddToAllocationTreeUUID, serializedContent)
    
    val notifyStores = newNodePointer.storePointers.map(sp => DataStoreID(newNodePointer.poolUUID, sp.poolIndex)).toSet
    transaction.addNotifyOnResolution(notifyStores)
  }
  
  class AddToAllocationTree(
      val system: AspenSystem,
      val storagePoolDefinitionPointer:KeyValueObjectPointer, 
      val newNodePointer:ObjectPointer)(implicit ec: ExecutionContext) extends FinalizationAction {
    
    val complete = system.getRetryStrategy(BasicAspenSystem.FinalizationActionRetryStrategyUUID).retryUntilSuccessful {
      //
      // TODO: getStoragePool will forever fail  if the pool description object is deleted (old Tx could be recovered after pool is deleted)
      //       detect this condition and return success to retryUntilSuccessful
      //
      implicit val tx = system.newTransaction()
      
      val fcommit = for {
        pool <- system.getStoragePool(storagePoolDefinitionPointer)
        tree <- pool.getAllocationTree(system.retryStrategy)
        commitReady <- tree.put(newNodePointer.uuid, newNodePointer.toArray)
        result <- tx.commit()
      } yield ()
      
      fcommit.failed.foreach(reason => tx.invalidateTransaction(reason))
      
      fcommit
    }
  }
}

class AllocationFinalizationAction extends FinalizationActionHandler {
  
  import AllocationFinalizationAction._
  
  val typeUUID: UUID = AddToAllocationTreeUUID
  
  override def createAction(
      system: AspenSystem, 
      txd: TransactionDescription,
      serializedActionData: Array[Byte])(implicit ec: ExecutionContext): FinalizationAction = {

    val fa = BaseCodec.decodeAllocationFinalizationActionContent(serializedActionData)
    
    new AddToAllocationTree(system, fa.storagePoolDefinitionPointer, fa.newNodePointer)      
  }
}

