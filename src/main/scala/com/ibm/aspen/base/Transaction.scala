package com.ibm.aspen.base

import java.util.UUID
import scala.concurrent.Future
import com.ibm.aspen.core.objects.ObjectPointer
import com.ibm.aspen.core.objects.ObjectRevision
import com.ibm.aspen.core.objects.ObjectRefcount
import java.nio.ByteBuffer

trait Transaction {
  
  def append(objectPointer: ObjectPointer, requiredRevision: ObjectRevision, data: ByteBuffer): Unit
  def overwrite(objectPointer: ObjectPointer, requiredRevision: ObjectRevision, data: ByteBuffer): Unit
  def setRefcount(objectPointer: ObjectPointer, requiredRefcount: ObjectRefcount, refcount: ObjectRefcount): Unit
  
  def append(objectPointer: ObjectPointer, requiredRevision: ObjectRevision, data: Array[Byte]): Unit = append(objectPointer, requiredRevision, ByteBuffer.wrap(data))
  def overwrite(objectPointer: ObjectPointer, requiredRevision: ObjectRevision, data: Array[Byte]): Unit = overwrite(objectPointer, requiredRevision, ByteBuffer.wrap(data))
  
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
  def commit(): Future[Unit]
  
}