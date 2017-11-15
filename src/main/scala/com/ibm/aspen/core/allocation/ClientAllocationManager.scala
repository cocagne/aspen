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
               poolUUID: UUID,
               newObjectUUID: UUID,
               objectSize: Option[Int],
               objectIDA: IDA,
               objectData: Map[Byte,DataBuffer], // Map DataStore pool index -> store-specific ObjectData
               initialRefcount: ObjectRefcount,
               allocationTransactionUUID: UUID,
               allocatingObject: ObjectPointer,
               allocatingObjectRevision: ObjectRevision): Future[Either[Map[Byte,AllocationErrors.Value], ObjectPointer]] = {
    
    val driver = driverFactory.create(clientMessenger, poolUUID, newObjectUUID, objectSize, objectIDA, objectData, initialRefcount,
                                      allocationTransactionUUID, allocatingObject, allocatingObjectRevision)
                                      
    synchronized { outstandingAllocations += (allocationTransactionUUID -> driver) }
    
    driver.futureResult onComplete {
      case _ => synchronized { outstandingAllocations -= allocationTransactionUUID }
    }
    
    driver.start()
    
    driver.futureResult
  }
}