package com.ibm.aspen.base.impl

import com.ibm.aspen.base.ObjectAllocater
import com.ibm.aspen.core.objects.ObjectPointer
import com.ibm.aspen.core.objects.ObjectRevision
import java.util.UUID
import com.ibm.aspen.core.ida.IDA
import com.ibm.aspen.core.DataBuffer
import com.ibm.aspen.core.HLCTimestamp
import scala.concurrent.ExecutionContext
import scala.concurrent.Future
import com.ibm.aspen.base.Transaction
import com.ibm.aspen.core.objects.DataObjectPointer
import com.ibm.aspen.core.objects.keyvalue.KeyValueOperation
import com.ibm.aspen.core.objects.KeyValueObjectPointer

class SinglePoolObjectAllocater(
    val system: BasicAspenSystem,
    val poolUUID: UUID,
    val maxObjectSize: Option[Int],
    val objectIDA: IDA) extends ObjectAllocater {
 
  override def allocateDataObject(
      allocatingObject: ObjectPointer,
      allocatingObjectRevision: ObjectRevision,
      initialContent: DataBuffer,
      afterTimestamp: Option[HLCTimestamp] = None)(implicit t: Transaction, ec: ExecutionContext): Future[DataObjectPointer] = {
    system.lowLevelAllocateDataObject(allocatingObject, allocatingObjectRevision, poolUUID, maxObjectSize, objectIDA, initialContent, afterTimestamp)
  }
  
  override def allocateKeyValueObject(
      allocatingObject: ObjectPointer,
      allocatingObjectRevision: ObjectRevision,
      initialContent: List[KeyValueOperation],
      afterTimestamp: Option[HLCTimestamp] = None)(implicit t: Transaction, ec: ExecutionContext): Future[KeyValueObjectPointer] = {
    system.lowLevelAllocateKeyValueObject(allocatingObject, allocatingObjectRevision, poolUUID, maxObjectSize, objectIDA, initialContent, afterTimestamp)
  }
}