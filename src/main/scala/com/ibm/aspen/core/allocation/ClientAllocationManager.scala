package com.ibm.aspen.core.allocation

import com.ibm.aspen.core.network.ClientSideAllocationMessenger
import scala.concurrent.ExecutionContext
import java.util.UUID
import com.ibm.aspen.core.ida.IDA
import com.ibm.aspen.core.objects.ObjectRefcount
import com.ibm.aspen.core.objects.ObjectPointer
import com.ibm.aspen.core.objects.ObjectRevision
import scala.concurrent.Future
import java.nio.ByteBuffer
import com.ibm.aspen.core.network.ClientSideAllocationMessageReceiver
import com.ibm.aspen.core.DataBuffer
import com.ibm.aspen.core.HLCTimestamp
import com.ibm.aspen.core.objects.DataObjectPointer
import com.ibm.aspen.base.Transaction
import com.ibm.aspen.base.StoragePool
import com.ibm.aspen.base.impl.AllocationFinalizationAction

class ClientAllocationManager(
    val clientMessenger: ClientSideAllocationMessenger,
    val driverFactory: AllocationDriver.Factory)(implicit ec: ExecutionContext) extends ClientSideAllocationMessageReceiver {
  
  private[this] var outstandingAllocations = Map[UUID, AllocationDriver]()
  
  def receive(m: AllocateResponse): Unit = { 
    synchronized { outstandingAllocations.get(m.allocationTransactionUUID) } foreach {
      driver => driver.receiveAllocationResult(m.fromStoreId, m.allocationTransactionUUID, m.result)
    }  
  }
  
  def allocate(messenger: ClientSideAllocationMessenger,
               transaction: Transaction,
               pool: StoragePool,
               newObjectUUID: UUID,
               objectSize: Option[Int],
               objectIDA: IDA,
               objectData: Map[Byte,DataBuffer], // Map DataStore pool index -> store-specific ObjectData
               timestamp: HLCTimestamp,
               initialRefcount: ObjectRefcount,
               allocatingObject: ObjectPointer,
               allocatingObjectRevision: ObjectRevision): Future[Either[Map[Byte,AllocationErrors.Value], ObjectPointer]] = {
    
    val driver = driverFactory.create(clientMessenger, pool.uuid, newObjectUUID, objectSize, objectIDA, objectData, timestamp, initialRefcount,
                                      transaction.uuid, allocatingObject, allocatingObjectRevision)
                                      
    synchronized { outstandingAllocations += (transaction.uuid -> driver) }
    
    driver.futureResult onComplete {
      case _ => synchronized { outstandingAllocations -= transaction.uuid }
    }
    
    driver.start()
    
    driver.futureResult map { eresult => eresult match {
      case Left(err) => Left(err)
      case Right(newObjectPtr) => 
        AllocationFinalizationAction.addToAllocationTree(transaction, pool.poolDefinitionPointer, newObjectPtr)
        Right(newObjectPtr)
    }}
  }
}