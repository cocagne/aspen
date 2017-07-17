package com.ibm.aspen.base

import java.util.UUID
import com.ibm.aspen.core.ida.IDA
import com.ibm.aspen.core.objects.ObjectPointer
import com.ibm.aspen.core.objects.ObjectRevision
import com.ibm.aspen.core.objects.ObjectRefcount
import java.nio.ByteBuffer

trait AspenObject {
  val pointer: ObjectPointer
  val revision: ObjectRevision
  val refcount: ObjectRefcount
    
  def uuid = pointer.uuid
  def poolUUID: UUID = pointer.poolUUID
  def maxSize: Option[Int] = pointer.size
  def ida: IDA = pointer.ida
  
  /** Returns a new read-only ByteBuffer containing the object's content */
  def content(): ByteBuffer
}