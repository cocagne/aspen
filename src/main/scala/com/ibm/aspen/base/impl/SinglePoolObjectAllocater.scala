package com.ibm.aspen.base.impl

import java.nio.ByteBuffer
import java.util.UUID

import com.ibm.aspen.base.{AspenSystem, ObjectAllocater, Transaction}
import com.ibm.aspen.core.DataBuffer
import com.ibm.aspen.core.allocation.AllocationRevisionGuard
import com.ibm.aspen.core.ida.IDA
import com.ibm.aspen.core.objects.{DataObjectPointer, KeyValueObjectPointer}
import com.ibm.aspen.core.objects.keyvalue.KeyValueOperation

import scala.concurrent.{ExecutionContext, Future}

class SinglePoolObjectAllocater(
    val system: AspenSystem,
    val allocaterUUID: UUID,
    val poolUUID: UUID,
    val maxObjectSize: Option[Int],
    val objectIDA: IDA) extends ObjectAllocater {
  
  def serialize(): Array[Byte] = {
    val arr = new Array[Byte](16)
    val bb = ByteBuffer.wrap(arr)
    bb.putLong(poolUUID.getMostSignificantBits)
    bb.putLong(poolUUID.getLeastSignificantBits)
    arr
  }

  def allocateDataObject(revisionGuard: AllocationRevisionGuard,
                         initialContent: DataBuffer)(implicit t: Transaction, ec: ExecutionContext): Future[DataObjectPointer] = {
    system.lowLevelAllocateDataObject(revisionGuard, poolUUID, maxObjectSize, objectIDA, initialContent)
  }

  def allocateKeyValueObject(revisionGuard: AllocationRevisionGuard,
                             initialContent: List[KeyValueOperation])(implicit t: Transaction, ec: ExecutionContext): Future[KeyValueObjectPointer] = {
    system.lowLevelAllocateKeyValueObject(revisionGuard, poolUUID, maxObjectSize, objectIDA, initialContent)
  }
}