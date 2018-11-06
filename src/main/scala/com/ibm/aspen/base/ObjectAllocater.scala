package com.ibm.aspen.base

import java.util.UUID

import com.ibm.aspen.core.DataBuffer
import com.ibm.aspen.core.allocation.{AllocationRevisionGuard, KeyValueAllocationRevisionGuard, ObjectAllocationRevisionGuard}
import com.ibm.aspen.core.ida.IDA
import com.ibm.aspen.core.objects.{DataObjectPointer, KeyValueObjectPointer, ObjectPointer, ObjectRevision}
import com.ibm.aspen.core.objects.keyvalue.{Insert, Key, KeyValueOperation}

import scala.concurrent.{ExecutionContext, Future}

trait ObjectAllocater {
  
  val system: AspenSystem
  
  val maxObjectSize: Option[Int]
  val objectIDA: IDA
  
  val allocaterUUID: UUID
  
  def serialize(): Array[Byte]

  def allocateDataObject(revisionGuard: AllocationRevisionGuard,
                         initialContent: DataBuffer)(implicit t: Transaction, ec: ExecutionContext): Future[DataObjectPointer]

  def allocateKeyValueObject(revisionGuard: AllocationRevisionGuard,
                             initialContent: List[KeyValueOperation])(implicit t: Transaction, ec: ExecutionContext): Future[KeyValueObjectPointer]
  
  def allocateDataObject(
      allocatingObject: ObjectPointer,
      allocatingObjectRevision: ObjectRevision,
      initialContent: DataBuffer)(implicit t: Transaction, ec: ExecutionContext): Future[DataObjectPointer] = {
    allocateDataObject(ObjectAllocationRevisionGuard(allocatingObject, allocatingObjectRevision), initialContent)
  }
  
  def allocateKeyValueObject(
      allocatingObject: ObjectPointer,
      allocatingObjectRevision: ObjectRevision,
      initialContent: List[KeyValueOperation])(implicit t: Transaction, ec: ExecutionContext): Future[KeyValueObjectPointer] = {
    allocateKeyValueObject(ObjectAllocationRevisionGuard(allocatingObject, allocatingObjectRevision), initialContent)
  }
  
  def allocateKeyValueObject(
      allocatingObject: ObjectPointer,
      allocatingObjectRevision: ObjectRevision,
      initialContent: Map[Key,Array[Byte]])(implicit t: Transaction, ec: ExecutionContext): Future[KeyValueObjectPointer] = {
    val ins = initialContent.foldLeft(List[KeyValueOperation]())( (l,x) => Insert(x._1, x._2, None, None) :: l )
    allocateKeyValueObject(allocatingObject, allocatingObjectRevision, ins) 
  }

  def allocateDataObject(
                          allocatingObject: KeyValueObjectPointer,
                          key: Key,
                          keyRevision: ObjectRevision,
                          initialContent: DataBuffer)(implicit t: Transaction, ec: ExecutionContext): Future[DataObjectPointer] = {
    allocateDataObject(KeyValueAllocationRevisionGuard(allocatingObject, key, keyRevision), initialContent)
  }

  def allocateKeyValueObject(
                              allocatingObject: KeyValueObjectPointer,
                              key: Key,
                              keyRevision: ObjectRevision,
                              initialContent: List[KeyValueOperation])(implicit t: Transaction, ec: ExecutionContext): Future[KeyValueObjectPointer] = {
    allocateKeyValueObject(KeyValueAllocationRevisionGuard(allocatingObject, key, keyRevision), initialContent)
  }

  def allocateKeyValueObject(
                              allocatingObject: KeyValueObjectPointer,
                              key: Key,
                              keyRevision: ObjectRevision,
                              initialContent: Map[Key,Array[Byte]])(implicit t: Transaction, ec: ExecutionContext): Future[KeyValueObjectPointer] = {
    val ins = initialContent.foldLeft(List[KeyValueOperation]())( (l,x) => Insert(x._1, x._2, None, None) :: l )
    allocateKeyValueObject(allocatingObject, key, keyRevision, ins)
  }
}