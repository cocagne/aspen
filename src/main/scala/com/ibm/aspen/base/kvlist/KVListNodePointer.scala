package com.ibm.aspen.base.kvlist

import com.ibm.aspen.core.objects.ObjectPointer
import com.ibm.aspen.core.objects.DataObjectPointer

case class KVListNodePointer(objectPointer:DataObjectPointer, minimum:Array[Byte]) {
  override def equals(other: Any): Boolean = other match {
    case rhs: KVListNodePointer => objectPointer == rhs.objectPointer && java.util.Arrays.equals(minimum, rhs.minimum)
    case _ => false
  }
  
  override def hashCode: Int = objectPointer.hashCode() ^ minimum.hashCode() 
}