package com.ibm.aspen.base.kvtree

import com.ibm.aspen.core.objects.ObjectPointer
import com.ibm.aspen.core.objects.ObjectRevision
import scala.concurrent.ExecutionContext
import com.ibm.aspen.base.Transaction
import scala.concurrent.Future
import com.ibm.aspen.base.kvlist.KVListNodeAllocater
import com.ibm.aspen.base.kvlist.KVListNodePointer
import java.util.UUID
import com.ibm.aspen.core.objects.DataObjectPointer

trait KVTreeNodeAllocater {
  
  val allocationPolicyUUID: UUID
  
  def allocateRootTierNode(
      targetObject: ObjectPointer, targetRevision: ObjectRevision, 
      newTier: Int, initialContent: List[KVListNodePointer])(implicit ec: ExecutionContext, t: Transaction): Future[DataObjectPointer]
  
  def allocateRootLeafNode(
      targetObject: ObjectPointer, targetRevision: ObjectRevision)(implicit ec: ExecutionContext, t: Transaction): Future[DataObjectPointer]
  
  def getListNodeAllocaterForTier(tier: Int): KVListNodeAllocater
}

