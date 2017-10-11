package com.ibm.aspen.base.impl

import java.util.UUID
import com.ibm.aspen.base.Transaction
import com.ibm.aspen.core.objects.ObjectPointer

object AllocationFinalizationAction {
  val AddToAllocationTree = UUID.fromString("909ce37d-a138-44a5-9498-f56095827cdf")
  
  val supportedUUIDs: Set[UUID] = Set(AddToAllocationTree)
  
  def addToAllocationTree(transaction: Transaction, storagePoolPointer:ObjectPointer, newNodePointer:ObjectPointer): Unit = {
    //val serializedContent = KVTreeCodec.encodeInsertIntoUpperTierFinalizationAction(tree, targetTier, nodePointer)
    //transaction.addFinalizationAction(InsertIntoUpperTierUUID, serializedContent)
  }
}

class AllocationFinalizationAction {
  
}