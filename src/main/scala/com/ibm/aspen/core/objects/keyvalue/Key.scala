package com.ibm.aspen.core.objects.keyvalue

final case class Key(bytes: Array[Byte]) {
  
  override def equals(other: Any): Boolean = other match {
    case that: Key => java.util.Arrays.equals(bytes, that.bytes)
    case _ => false
  }
  
  override def hashCode: Int = java.util.Arrays.hashCode(bytes)
}