package com.ibm.aspen.base.kvtree


private[kvtree] class KVTreeNodePointerOrdering(val keyCompare: (Array[Byte], Array[Byte]) => Int) extends Ordering[KVTreeNodePointer] {
  def compare(a:KVTreeNodePointer, b:KVTreeNodePointer): Int = {
    val keyComp = keyCompare(a.minimum, b.minimum)
      
    if (keyComp == 0) 
      a.objectPointer.uuid.compareTo(b.objectPointer.uuid)
    else
      keyComp
  }
}
