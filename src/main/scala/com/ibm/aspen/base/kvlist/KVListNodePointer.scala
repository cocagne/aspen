package com.ibm.aspen.base.kvlist

import com.ibm.aspen.core.objects.ObjectPointer

case class KVListNodePointer(objectPointer:ObjectPointer, minimum:Array[Byte]) {
  override def equals(other: Any): Boolean = other match {
    case rhs: KVListNodePointer => objectPointer == rhs.objectPointer && java.util.Arrays.equals(minimum, rhs.minimum)
    case _ => false
  }
  
  override def hashCode: Int = objectPointer.hashCode() ^ minimum.hashCode() 
}