package com.ibm.aspen.base.kvtree

import com.ibm.aspen.core.objects.ObjectPointer

private[kvtree] case class KVTreeNodePointer(objectPointer:ObjectPointer, minimum:Array[Byte]) {
  override def equals(other: Any): Boolean = other match {
    case rhs: KVTreeNodePointer => objectPointer == rhs.objectPointer && java.util.Arrays.equals(minimum, rhs.minimum)
    case _ => false
  }
  
  override def hashCode: Int = objectPointer.hashCode() ^ minimum.hashCode() 
}
