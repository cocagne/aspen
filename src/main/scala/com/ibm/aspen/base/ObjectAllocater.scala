package com.ibm.aspen.base

import com.ibm.aspen.core.objects.ObjectPointer
import com.ibm.aspen.core.objects.ObjectRevision
import java.util.UUID
import com.ibm.aspen.core.ida.IDA
import com.ibm.aspen.core.DataBuffer
import com.ibm.aspen.core.HLCTimestamp
import scala.concurrent.ExecutionContext
import scala.concurrent.Future

trait ObjectAllocater {
  
  val system: AspenSystem
  
  val objectSize: Option[Int]
  val objectIDA: IDA
  
  def allocateObject(
      allocatingObject: ObjectPointer,
      allocatingObjectRevision: ObjectRevision,
      initialContent: DataBuffer,
      afterTimestamp: Option[HLCTimestamp] = None)(implicit t: Transaction, ec: ExecutionContext): Future[ObjectPointer]
  
  def allocateObject(
      allocatingObject: ObjectPointer,
      allocatingObjectRevision: ObjectRevision,
      initialContent: Array[Byte])(implicit t: Transaction, ec: ExecutionContext): Future[ObjectPointer] = {
    allocateObject(allocatingObject, allocatingObjectRevision, DataBuffer(initialContent), None)
  }
  
  def allocateObject(
      allocatingObject: ObjectPointer,
      allocatingObjectRevision: ObjectRevision,
      initialContent: Array[Byte],
      afterTimestamp: Option[HLCTimestamp])(implicit t: Transaction, ec: ExecutionContext): Future[ObjectPointer] = {
    allocateObject(allocatingObject, allocatingObjectRevision, DataBuffer(initialContent), afterTimestamp)
  }
}