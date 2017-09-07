package com.ibm.aspen.base.kvtree

import com.ibm.aspen.base.kvlist.KVListNodePointer
import com.ibm.aspen.base.kvlist.KVListNode
import com.ibm.aspen.core.objects.ObjectPointer

trait KVTreeNodeCache {
  def getCachedNode(tier: Int, ptr: ObjectPointer): Option[KVListNode] = None
  def updateCachedNode(tier: Int, node: KVListNode): Unit = ()
  def dropCachedNode(node: KVListNode): Unit = ()
}