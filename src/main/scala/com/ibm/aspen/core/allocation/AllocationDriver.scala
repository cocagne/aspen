package com.ibm.aspen.core.allocation

import com.ibm.aspen.core.network.ClientSideAllocationMessenger
import java.util.UUID
import scala.concurrent.Promise
import com.ibm.aspen.core.objects.ObjectPointer
import com.ibm.aspen.core.objects.ObjectRefcount
import com.ibm.aspen.core.objects.ObjectRevision
import com.ibm.aspen.core.data_store.DataStoreID
import com.ibm.aspen.core.objects.StorePointer
import com.ibm.aspen.core.ida.IDA
import java.nio.ByteBuffer
import scala.concurrent.Future
import com.ibm.aspen.core.DataBuffer
import com.ibm.aspen.core.HLCTimestamp

/** Handles the sending and receiving of messages used to allocate a new object. 
 *  
 *  This class provides no error handling or message retransmissions so it is suitable for direct use
 *  only in unit/integration tests where message loss is not an issue. Subclasses should be used to
 *  provide error-handling strategies.
 * 
 */
trait AllocationDriver {
    
  def futureResult: Future[Either[Map[Byte,AllocationErrors.Value], ObjectPointer]]
  
  /** Initiates the allocation process */
  def start(): Unit
  
  def receiveAllocationResult(fromStoreId: DataStoreID, 
                              allocationTransactionUUID: UUID, 
                              result: Either[AllocationErrors.Value, List[AllocateResponse.Allocated]]): Unit 
}

object AllocationDriver {
  trait Factory {
    def create(messenger: ClientSideAllocationMessenger,
               poolUUID: UUID,
               newObjectUUID: UUID,
               objectSize: Option[Int],
               objectIDA: IDA,
               objectData: Map[Byte,DataBuffer], // Map DataStore pool index -> store-specific ObjectData
               options: AllocationOptions,
               timestamp: HLCTimestamp,
               initialRefcount: ObjectRefcount,
               allocationTransactionUUID: UUID,
               allocatingObject: ObjectPointer,
               allocatingObjectRevision: ObjectRevision): AllocationDriver 
  }
  
}