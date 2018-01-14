package com.ibm.aspen.base.kvtree

import com.ibm.aspen.base.kvlist.KVListNodePointer
import com.ibm.aspen.base.kvlist.KVListNode
import com.ibm.aspen.core.objects.ObjectPointer
import com.ibm.aspen.core.objects.DataObjectPointer

trait KVTreeNodeCache {
  def getCachedNode(tier: Int, ptr: DataObjectPointer): Option[KVListNode] = None
  def updateCachedNode(tier: Int, node: KVListNode): Unit = ()
  def dropCachedNode(node: KVListNode): Unit = ()
}