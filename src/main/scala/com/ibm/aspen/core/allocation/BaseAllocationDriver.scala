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
import com.ibm.aspen.core.DataBuffer
import com.ibm.aspen.core.HLCTimestamp
import com.ibm.aspen.core.objects.DataObjectPointer
import com.ibm.aspen.core.objects.KeyValueObjectPointer

/** Handles the sending and receiving of messages used to allocate a new object. 
 *  
 *  This class provides no error handling or message retransmissions so it is suitable for direct use
 *  only in unit/integration tests where message loss is not an issue. Subclasses should be used to
 *  provide error-handling strategies.
 *  
 *  TODO: Add support for multiple objects
 * 
 */
class BaseAllocationDriver (
    val messenger: ClientSideAllocationMessenger,
    val poolUUID: UUID,
    val newObjectUUID: UUID,
    val objectSize: Option[Int],
    val objectIDA: IDA,
    val objectData: Map[Byte,DataBuffer], // Map DataStore pool index -> store-specific ObjectData
    val options: AllocationOptions,
    val timestamp: HLCTimestamp,
    val initialRefcount: ObjectRefcount,
    val allocationTransactionUUID: UUID,
    val allocatingObject: ObjectPointer,
    val allocatingObjectRevision: ObjectRevision
    ) extends AllocationDriver {
  
  private[this] val promise = Promise[Either[Map[Byte,AllocationErrors.Value], ObjectPointer]]
  
  def futureResult = promise.future
  
  private[this] var responses =  Map[Byte, Either[AllocationErrors.Value,  List[AllocateResponse.Allocated]]]()
  
  def shutdown(): Unit = {}
  
  /** Initiates the allocation process */
  def start() = sendAllocationMessages()
  
  protected def sendAllocationMessages(): Unit = {
    val toSend = synchronized { objectData.filter( t => !responses.contains(t._1) ) }
    
    for ( (storeIndex, objectData) <- toSend ) {
      val storeId = DataStoreID(poolUUID, storeIndex)
      val newObjects = List( Allocate.NewObject(newObjectUUID, options, objectSize, initialRefcount, objectData) )
      val msg = Allocate(storeId, messenger.clientId, newObjects, timestamp,
                         allocationTransactionUUID, allocatingObject, allocatingObjectRevision)
                         
      messenger.send(storeId, msg)
    }
  }
  
  def receiveAllocationResult(fromStoreId: DataStoreID, 
                              allocationTransactionUUID: UUID, 
                              result: Either[AllocationErrors.Value,  List[AllocateResponse.Allocated]]): Unit = synchronized {
    if (promise.isCompleted)
      return // Already done, nothing left to do
      
    if ( !responses.contains(fromStoreId.poolIndex) )
      responses += (fromStoreId.poolIndex -> result)
      
    if (responses.size == objectData.size) {
      var errors = Map[Byte,AllocationErrors.Value]()
      var pointers = List[StorePointer]()
      
      responses.foreach(t => t._2 match {
        case Right(lst) => pointers = lst.head.storePointer :: pointers
        case Left(err) => errors += (t._1 -> err)
      })
      
      if (errors.isEmpty) {
        val sortedPointersArray = pointers.sortBy(sp => sp.poolIndex).toArray
        val op = options match {
          case _: DataAllocationOptions => new DataObjectPointer(newObjectUUID, poolUUID, objectSize, objectIDA, sortedPointersArray)
          case _: KeyValueAllocationOptions => new KeyValueObjectPointer(newObjectUUID, poolUUID, objectSize, objectIDA, sortedPointersArray)
        }
        promise.success(Right(op))
      } else 
        promise.success(Left(errors))
    }
  }
}

object BaseAllocationDriver {
  object Factory extends AllocationDriver.Factory {
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
               allocatingObjectRevision: ObjectRevision): BaseAllocationDriver = {
      new BaseAllocationDriver(messenger, poolUUID, newObjectUUID, objectSize, objectIDA, objectData, options, timestamp,  
                           initialRefcount, allocationTransactionUUID, allocatingObject, allocatingObjectRevision)
    }
  }
  
  val NoErrorRecoveryAllocationDriver = Factory
}