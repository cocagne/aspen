package com.ibm.aspen.base.kvlist

import scala.concurrent.ExecutionContext
import com.ibm.aspen.base.Transaction
import scala.concurrent.Future
import com.ibm.aspen.core.objects.ObjectPointer
import java.util.UUID
import com.ibm.aspen.core.objects.ObjectRevision
import java.nio.ByteBuffer
import com.ibm.aspen.core.DataBuffer
import com.ibm.aspen.core.HLCTimestamp

trait KVListNodeAllocater {
  val allocationPolicyUUID: UUID
  
  def allocate(
      targetObject:ObjectPointer, targetRevision: ObjectRevision, 
      initialContent: DataBuffer, timestamp: HLCTimestamp)(implicit ec: ExecutionContext, t: Transaction): Future[ObjectPointer]
  
  val nodeSizeLimit: Int
}
