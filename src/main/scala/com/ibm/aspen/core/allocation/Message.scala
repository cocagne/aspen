package com.ibm.aspen.core.allocation

import com.ibm.aspen.core.network.Client
import java.util.UUID
import com.ibm.aspen.core.objects.ObjectRefcount
import com.ibm.aspen.core.objects.ObjectPointer
import com.ibm.aspen.core.objects.ObjectRevision
import com.ibm.aspen.core.data_store.DataStoreID
import com.ibm.aspen.core.objects.StorePointer

sealed abstract class Message

final case class Allocate(
    toStore: DataStoreID,
    fromClient: Client,
    newObjectUUID: UUID,
    objectSize: Option[Int],
    objectData: Array[Byte],
    initialRefcount: ObjectRefcount,
    allocationTransactionUUID: UUID,
    allocatingObject: ObjectPointer,
    allocatingObjectRevision: ObjectRevision
    ) extends Message
    
final case class AllocateResponse(
    fromStoreId: DataStoreID,
    allocationTransactionUUID: UUID,
    result: Either[AllocationError.Value, StorePointer]) extends Message
    