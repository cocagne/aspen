package com.ibm.aspen.base

import com.ibm.aspen.core.objects.ObjectPointer
import com.ibm.aspen.core.objects.ObjectRevision
import scala.concurrent.Future
import com.ibm.aspen.core.ida.IDA
import java.nio.ByteBuffer

trait StoragePool {
  def width: Int
  
  def allocateInto(targetPointer:ObjectPointer, 
                   targetRevision: ObjectRevision,
                   newObjectIDA: IDA,
                   newObjectContent: ByteBuffer,
                   writeToTarget:(ObjectPointer, ObjectRevision, ObjectPointer, Transaction) => Unit): Future[AspenObject]
}