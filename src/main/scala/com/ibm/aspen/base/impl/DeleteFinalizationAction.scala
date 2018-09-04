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
import com.ibm.aspen.core.transaction.SerializedFinalizationAction
import org.apache.logging.log4j.scala.Logging

// Note - The deletion finalization action requires the same information as the allocation FA so we just re-use the serialization
//        for that
object DeleteFinalizationAction {
  val RemoveFromAllocationTreeUUID = UUID.fromString("b984d680-e4b9-42b6-b288-9f93194815c9")
  
  def createSerializedFA(victim: ObjectPointer): SerializedFinalizationAction = {
    SerializedFinalizationAction(RemoveFromAllocationTreeUUID, victim.toArray)
  }
  
  class RemoveFromAllocationTree(
      val system: AspenSystem,
      val victim:ObjectPointer)(implicit ec: ExecutionContext) extends FinalizationAction with Logging {
    
    val complete = system.getRetryStrategy(BasicAspenSystem.FinalizationActionRetryStrategyUUID).retryUntilSuccessful {
      //
      // TODO: getStoragePool will forever fail if the pool description object is deleted (old Tx could be recovered after pool is deleted)
      //       detect this condition and return success to retryUntilSuccessful
      //
      implicit val tx = system.newTransaction()
      
      val fcommit = for {
        pool <- system.getStoragePool(victim.poolUUID)
        tree <- pool.getAllocationTree(system.retryStrategy)
        commitReady <- tree.prepareDelete(victim.uuid)
        result <- tx.commit()
      } yield {
        logger.info(s"Removed from allocation tree: ${victim.objectType}:${victim.uuid}")
      }
      
      fcommit.failed.foreach(reason => tx.invalidateTransaction(reason))
      
      fcommit
    }
  }
}

class DeleteFinalizationAction extends FinalizationActionHandler {
  
  import DeleteFinalizationAction._
  
  val typeUUID: UUID = RemoveFromAllocationTreeUUID
  
  override def createAction(
      system: AspenSystem, 
      txd: TransactionDescription,
      serializedActionData: Array[Byte])(implicit ec: ExecutionContext): FinalizationAction = {

    new RemoveFromAllocationTree(system, ObjectPointer(serializedActionData))      
  }
}

