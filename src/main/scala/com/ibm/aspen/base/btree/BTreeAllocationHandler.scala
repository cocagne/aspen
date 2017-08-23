package com.ibm.aspen.base.btree

import com.ibm.aspen.base.Transaction
import scala.concurrent.Future
import com.ibm.aspen.core.objects.ObjectPointer
import java.nio.ByteBuffer

trait BTreeAllocationHandler {
  def tierSizeLimit: Int
  
  def allocateNewNode(initialContent: ByteBuffer)(implicit t: Transaction): Future[ObjectPointer]
  
  def freeNode(op: ObjectPointer)(implicit t: Transaction): Unit
}