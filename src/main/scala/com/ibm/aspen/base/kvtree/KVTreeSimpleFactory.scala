package com.ibm.aspen.base.kvtree

import com.ibm.aspen.base.AspenSystem
import java.util.UUID
import com.ibm.aspen.base.kvlist.KVListNodeAllocater
import com.ibm.aspen.core.objects.ObjectPointer
import com.ibm.aspen.core.objects.ObjectRevision
import java.nio.ByteBuffer
import scala.concurrent.ExecutionContext
import com.ibm.aspen.base.Transaction
import scala.concurrent.Future
import com.ibm.aspen.base.kvlist.KVListNodePointer
import com.ibm.aspen.core.ida.IDA
import com.ibm.aspen.core.DataBuffer
import com.ibm.aspen.core.HLCTimestamp

class KVTreeSimpleFactory(
    system: AspenSystem,
    treeAllocationPolicyUUID: UUID,
    storagePoolUUID: UUID,
    nodeIDA: IDA,
    nodeSize: Int,
    nodeCache: KVTreeNodeCache,
    keyComparisonStrategy: KVTree.KeyComparison.Value) extends KVTreeFactory {
  
  def createTree(treeDefinitionObjectPointer: ObjectPointer)(implicit ec: ExecutionContext): Future[KVTree] = system.readObject(treeDefinitionObjectPointer) map {
    osd => 
      val td = KVTreeCodec.decodeTreeDefinition(osd.data)
      new KVTree(treeDefinitionObjectPointer, osd.revision, new TreeAllocater, nodeCache, keyComparisonStrategy, td.tiers, system)  
  }
  
  class TreeListAllocater extends KVListNodeAllocater {
       
    val allocationPolicyUUID: UUID = treeAllocationPolicyUUID
    
    val nodeSizeLimit = nodeSize
    
    def allocate(
        targetObject:ObjectPointer, 
        targetRevision: ObjectRevision, 
        initialContent: DataBuffer,
        timestamp: HLCTimestamp)(implicit ec: ExecutionContext, t: Transaction): Future[ObjectPointer] = {
      system.allocateObject(targetObject, ObjectRevision(0,0), storagePoolUUID, None, nodeIDA, initialContent, Some(timestamp))
    }
  }
  
  class TreeAllocater extends KVTreeNodeAllocater {

    val allocationPolicyUUID: UUID = treeAllocationPolicyUUID
    
    def allocateRootTierNode(
        targetObject: ObjectPointer, 
        targetRevision: ObjectRevision, 
        newTier: Int, 
        initialContent: List[KVListNodePointer])(implicit ec: ExecutionContext, t: Transaction): Future[ObjectPointer] = {
      system.allocateObject(targetObject, ObjectRevision(0,0), storagePoolUUID, None, nodeIDA, KVTreeCodec.encode(initialContent, None))
    }
    
    def allocateRootLeafNode(
        targetObject: ObjectPointer, 
        targetRevision: ObjectRevision)(implicit ec: ExecutionContext, t: Transaction): Future[ObjectPointer] = {
      system.allocateObject(targetObject, ObjectRevision(0,0), storagePoolUUID, None, nodeIDA, DataBuffer(ByteBuffer.allocate(0)))
    }
    
    def getListNodeAllocaterForTier(tier: Int): KVListNodeAllocater = new TreeListAllocater
  }
}