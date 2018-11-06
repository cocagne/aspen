package com.ibm.aspen.core.allocation

import java.util.UUID

import com.ibm.aspen.core.{DataBuffer, HLCTimestamp}
import com.ibm.aspen.core.data_store.DataStoreID
import com.ibm.aspen.core.ida.IDA
import com.ibm.aspen.core.network.ClientSideAllocationMessenger
import com.ibm.aspen.core.objects.{ObjectPointer, ObjectRefcount, StorePointer}

import scala.concurrent.Future

/** Handles the sending and receiving of messages used to allocate a new object. 
 *  
 *  This class provides no error handling or message retransmissions so it is suitable for direct use
 *  only in unit/integration tests where message loss is not an issue. Subclasses should be used to
 *  provide error-handling strategies.
 * 
 */
trait AllocationDriver {
    
  def futureResult: Future[Either[Map[Byte,AllocationErrors.Value], ObjectPointer]]
  
  /** Immediately cancels all future activity scheduled for execution */
  def shutdown(): Unit
  
  /** Initiates the allocation process */
  def start(): Unit
  
  def receiveAllocationResult(fromStoreId: DataStoreID, 
                              result: Either[AllocationErrors.Value, StorePointer]): Unit 
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
               revisionGuard: AllocationRevisionGuard): AllocationDriver
  }
  
}