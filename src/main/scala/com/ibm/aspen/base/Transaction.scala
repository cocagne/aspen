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
  
  /** Future to the result of the transaction. 
   *  
   *  The future successfully completes if the transaction commits. Otherwise it will fail with a TransactionError subclass.  
   */
  def commit(): Future[Unit]
  
}