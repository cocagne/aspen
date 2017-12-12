package com.ibm.aspen.core.allocation

import com.ibm.aspen.core.network.ClientID
import java.util.UUID
import com.ibm.aspen.core.objects.ObjectRefcount
import com.ibm.aspen.core.objects.ObjectPointer
import com.ibm.aspen.core.objects.ObjectRevision
import com.ibm.aspen.core.data_store.DataStoreID
import com.ibm.aspen.core.objects.StorePointer
import java.nio.ByteBuffer
import com.ibm.aspen.core.DataBuffer
import com.ibm.aspen.core.transaction.TransactionStatus

sealed abstract class Message

abstract class ClientMessage extends Message
abstract class StoreMessage extends Message

object Allocate {
  case class NewObject(
      newObjectUUID: UUID,
      objectSize: Option[Int],
      initialRefcount: ObjectRefcount,
      objectData: DataBuffer) {
    override def equals(other: Any): Boolean = other match {
      case rhs: Allocate.NewObject => 
        objectSize == rhs.objectSize && objectData.compareTo(rhs.objectData) == 0 &&
        initialRefcount == rhs.initialRefcount 
      case _ => false
    }
  }
      
}
final case class Allocate(
    toStore: DataStoreID,
    fromClient: ClientID,
    newObjects: List[Allocate.NewObject],
    allocationTransactionUUID: UUID,
    allocatingObject: ObjectPointer,
    allocatingObjectRevision: ObjectRevision
    ) extends ClientMessage {
  
  override def equals(other: Any): Boolean = other match {
    case rhs: Allocate => toStore == rhs.toStore && fromClient == rhs.fromClient && 
      newObjects == rhs.newObjects && allocationTransactionUUID == rhs.allocationTransactionUUID &&
      allocatingObject == rhs.allocatingObject && allocatingObjectRevision == rhs.allocatingObjectRevision
    case _ => false
  }
}

object AllocateResponse {
  case class Allocated(newObjectUUID: UUID, storePointer: StorePointer)
}
    
final case class AllocateResponse(
    fromStoreId: DataStoreID,
    allocationTransactionUUID: UUID,
    result: Either[AllocationErrors.Value, List[AllocateResponse.Allocated]]) extends ClientMessage
    
final case class AllocationStatusRequest(
    to: DataStoreID,
    from: DataStoreID,
    primaryObject: ObjectPointer,
    allocationTransactionUUID: UUID) extends StoreMessage
    
final case class AllocationStatusReply(
    to: DataStoreID,
    from: DataStoreID,
    allocationTransactionUUID: UUID,
    transactionStatus: Option[TransactionStatus.Value], // If None, the transaction is unknown
    objectStatus: AllocationObjectStatus
    ) extends StoreMessage
    