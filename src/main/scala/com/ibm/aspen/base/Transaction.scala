package com.ibm.aspen.base

import java.util.UUID
import scala.concurrent.Future
import com.ibm.aspen.core.objects.ObjectPointer
import com.ibm.aspen.core.objects.ObjectRevision
import com.ibm.aspen.core.objects.ObjectRefcount
import java.nio.ByteBuffer
import scala.concurrent.ExecutionContext
import com.ibm.aspen.core.DataBuffer
import com.ibm.aspen.core.data_store.DataStoreID

trait Transaction {
  
  val uuid: UUID
  
  // All returns are what the new object revision/refcount will be if the transaction completes successfully
  def append(objectPointer: ObjectPointer, requiredRevision: ObjectRevision, data: DataBuffer): ObjectRevision
  def overwrite(objectPointer: ObjectPointer, requiredRevision: ObjectRevision, data: DataBuffer): ObjectRevision
  def setRefcount(objectPointer: ObjectPointer, requiredRefcount: ObjectRefcount, refcount: ObjectRefcount): ObjectRefcount
  
  def append(objectPointer: ObjectPointer, requiredRevision: ObjectRevision, data: Array[Byte]): ObjectRevision = append(objectPointer, requiredRevision, DataBuffer(data))
  def overwrite(objectPointer: ObjectPointer, requiredRevision: ObjectRevision, data: Array[Byte]): ObjectRevision = overwrite(objectPointer, requiredRevision, DataBuffer(data))
  
  /** Increments the overwrite count on the object revision by 1 but leaves the object data untouched */
  def bumpVersion(objectPointer: ObjectPointer, requiredRevision: ObjectRevision): ObjectRevision
  
  def addFinalizationAction(finalizationActionUUID: UUID, serializedContent: Array[Byte]): Unit
  
  def addNotifyOnResolution(storesToNotify: Set[DataStoreID]): Unit
  
  /* Only the first error will be propagated should multiple attempts are made to invalidate the transaction
   * 
   */
  def invalidateTransaction(reason: Throwable): Unit
  
  def result: Future[Unit]
  
  /** Begins the transaction commit process and returns a Future to its completion. This is the same future as
   *  returned by 'result' 
   *  
   *  The future successfully completes if the transaction commits. Otherwise it will fail with a TransactionError subclass.  
   */
  def commit()(implicit ec: ExecutionContext): Future[Unit]
  
}