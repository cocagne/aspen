package com.ibm.aspen.core.allocation

import com.ibm.aspen.core.network.AllocationMessenger
import java.util.UUID
import scala.concurrent.Promise
import com.ibm.aspen.core.objects.ObjectPointer
import com.ibm.aspen.core.objects.ObjectRefcount
import com.ibm.aspen.core.objects.ObjectRevision
import com.ibm.aspen.core.data_store.DataStoreID
import com.ibm.aspen.core.objects.StorePointer
import com.ibm.aspen.core.ida.IDA

/** Handles the sending and receiving of messages used to allocate a new object. 
 *  
 *  This class provides no error handling or message retransmissions so it is suitable for direct use
 *  only in unit/integration tests where message loss is not an issue. Subclasses should be used to
 *  provide error-handling strategies.
 * 
 */
class AllocationDriver (
    val messenger: AllocationMessenger,
    val poolUUID: UUID,
    val newObjectUUID: UUID,
    val objectSize: Option[Int],
    val objectIDA: IDA,
    val objectData: Map[Byte,Array[Byte]], // Map DataStore pool index -> store-specific ObjectData
    val initialRefcount: ObjectRefcount,
    val allocationTransactionUUID: UUID,
    val allocatingObject: ObjectPointer,
    val allocatingObjectRevision: ObjectRevision
    ){
  
  private[this] val promise = Promise[Either[Map[Byte,AllocationError.Value], ObjectPointer]]
  
  def futureResult = promise.future
  
  private[this] var responses =  Map[Byte, Either[AllocationError.Value, StorePointer]]()
  
  /** Initiates the allocation process */
  def start() = sendAllocationMessages()
  
  protected def sendAllocationMessages(): Unit = {
    val toSend = synchronized { objectData.filter( t => !responses.contains(t._1) ) }
    
    for ( (storeIndex, objectData) <- toSend ) {
      val storeId = DataStoreID(poolUUID, storeIndex)
      
      val msg = Allocate(messenger.client, newObjectUUID, objectSize, objectData, initialRefcount, allocationTransactionUUID,
                         allocatingObject, allocatingObjectRevision)
                         
      messenger.send(storeId, msg)
    }
  }
  
  def receiveAllocationResult(fromStoreId: DataStoreID, 
                              allocationTransactionUUID: UUID, 
                              result: Either[AllocationError.Value, StorePointer]): Unit = synchronized {
    if (promise.isCompleted)
      return // Already done, nothing left to do
      
    if ( !responses.contains(fromStoreId.poolIndex) )
      responses += (fromStoreId.poolIndex -> result)
      
    if (responses.size == objectData.size) {
      var errors = Map[Byte,AllocationError.Value]()
      var pointers = List[StorePointer]()
      
      responses.foreach(t => t._2 match {
        case Right(sp) => pointers = sp :: pointers
        case Left(err) => errors += (t._1 -> err)
      })
      
      if (errors.isEmpty)
        promise.success(Right(ObjectPointer(newObjectUUID, poolUUID, objectSize, objectIDA, pointers.toArray)))
      else 
        promise.success(Left(errors))
    }
  }
}

object AllocationDriver {
  trait Factory {
    def create(messenger: AllocationMessenger,
               poolUUID: UUID,
               newObjectUUID: UUID,
               objectSize: Option[Int],
               objectIDA: IDA,
               objectData: Map[Byte,Array[Byte]], // Map DataStore pool index -> store-specific ObjectData
               initialRefcount: ObjectRefcount,
               allocationTransactionUUID: UUID,
               allocatingObject: ObjectPointer,
               allocatingObjectRevision: ObjectRevision): AllocationDriver = {
      new AllocationDriver(messenger, poolUUID, newObjectUUID, objectSize, objectIDA, objectData, initialRefcount, 
                           allocationTransactionUUID, allocatingObject, allocatingObjectRevision)
    }
  }
  
  object NoErrorRecoveryAllocationDriver extends Factory {}
}