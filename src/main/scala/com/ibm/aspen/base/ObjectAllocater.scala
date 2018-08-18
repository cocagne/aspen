package com.ibm.aspen.base

import com.ibm.aspen.core.objects.ObjectPointer
import com.ibm.aspen.core.objects.ObjectRevision
import java.util.UUID
import com.ibm.aspen.core.ida.IDA
import com.ibm.aspen.core.DataBuffer
import com.ibm.aspen.core.HLCTimestamp
import scala.concurrent.ExecutionContext
import scala.concurrent.Future
import com.ibm.aspen.core.objects.DataObjectPointer
import com.ibm.aspen.core.objects.keyvalue.KeyValueOperation
import com.ibm.aspen.core.objects.KeyValueObjectPointer
import com.ibm.aspen.core.objects.keyvalue.Key
import com.ibm.aspen.core.objects.keyvalue.Insert

trait ObjectAllocater {
  
  val system: AspenSystem
  
  val maxObjectSize: Option[Int]
  val objectIDA: IDA
  
  val allocaterUUID: UUID
  
  def serialize(): Array[Byte]
  
  def allocateDataObject(
      allocatingObject: ObjectPointer,
      allocatingObjectRevision: ObjectRevision,
      initialContent: DataBuffer,
      afterTimestamp: Option[HLCTimestamp] = None)(implicit t: Transaction, ec: ExecutionContext): Future[DataObjectPointer]
  
  def allocateKeyValueObject(
      allocatingObject: ObjectPointer,
      allocatingObjectRevision: ObjectRevision,
      initialContent: List[KeyValueOperation],
      afterTimestamp: Option[HLCTimestamp] = None)(implicit t: Transaction, ec: ExecutionContext): Future[KeyValueObjectPointer]
  
  def allocateKeyValueObject(
      allocatingObject: ObjectPointer,
      allocatingObjectRevision: ObjectRevision,
      initialContent: Map[Key,Array[Byte]])(implicit t: Transaction, ec: ExecutionContext): Future[KeyValueObjectPointer] = {
    val ins = initialContent.foldLeft(List[KeyValueOperation]())( (l,x) => Insert(x._1, x._2, None, None) :: l )
    allocateKeyValueObject(allocatingObject, allocatingObjectRevision, ins) 
  }
}