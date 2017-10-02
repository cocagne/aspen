package com.ibm.aspen.core.allocation

import com.ibm.aspen.core.network.Client
import java.util.UUID
import com.ibm.aspen.core.objects.ObjectRefcount
import com.ibm.aspen.core.objects.ObjectPointer
import com.ibm.aspen.core.objects.ObjectRevision
import com.ibm.aspen.core.data_store.DataStoreID
import com.ibm.aspen.core.objects.StorePointer
import java.nio.ByteBuffer

sealed abstract class Message

final case class Allocate(
    toStore: DataStoreID,
    fromClient: Client,
    newObjectUUID: UUID,
    objectSize: Option[Int],
    objectData: ByteBuffer,
    initialRefcount: ObjectRefcount,
    allocationTransactionUUID: UUID,
    allocatingObject: ObjectPointer,
    allocatingObjectRevision: ObjectRevision
    ) extends Message {
  
  override def equals(other: Any): Boolean = other match {
    case rhs: Allocate => toStore == rhs.toStore && fromClient == rhs.fromClient && 
      objectSize == rhs.objectSize && objectData.compareTo(rhs.objectData) == 0 &&
      initialRefcount == rhs.initialRefcount && allocationTransactionUUID == rhs.allocationTransactionUUID &&
      allocatingObject == rhs.allocatingObject && allocatingObjectRevision == rhs.allocatingObjectRevision
    case _ => false
  }
}
    
final case class AllocateResponse(
    fromStoreId: DataStoreID,
    allocationTransactionUUID: UUID,
    result: Either[AllocationErrors.Value, StorePointer]) extends Message
    