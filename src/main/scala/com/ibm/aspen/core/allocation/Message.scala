package com.ibm.aspen.core.allocation

import java.util.UUID

import com.ibm.aspen.core.{DataBuffer, HLCTimestamp}
import com.ibm.aspen.core.data_store.DataStoreID
import com.ibm.aspen.core.network.ClientID
import com.ibm.aspen.core.objects.{ObjectPointer, ObjectRefcount, StorePointer}
import com.ibm.aspen.core.transaction.TransactionStatus

sealed abstract class Message

abstract sealed class ClientMessage extends Message
abstract sealed class StoreMessage extends Message

final case class Allocate(
    toStore: DataStoreID,
    fromClient: ClientID,
    newObjectUUID: UUID,
    options: AllocationOptions,
    objectSize: Option[Int],
    initialRefcount: ObjectRefcount,
    objectData: DataBuffer,
    timestamp: HLCTimestamp,
    allocationTransactionUUID: UUID,
    revisionGuard: AllocationRevisionGuard
    ) extends ClientMessage {
  
  override def equals(other: Any): Boolean = other match {
    case rhs: Allocate => toStore == rhs.toStore && fromClient == rhs.fromClient && 
      objectSize == rhs.objectSize && objectData.compareTo(rhs.objectData) == 0 &&
      initialRefcount == rhs.initialRefcount && timestamp.compareTo(rhs.timestamp) == 0 &&  
      allocationTransactionUUID == rhs.allocationTransactionUUID &&
      revisionGuard == rhs.revisionGuard
    case _ => false
  }
}

final case class AllocateResponse(
    fromStoreId: DataStoreID,
    allocationTransactionUUID: UUID,
    newObjectUUID: UUID,
    result: Either[AllocationErrors.Value, StorePointer]) extends ClientMessage

    