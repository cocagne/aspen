package com.ibm.aspen.core.objects

import java.nio.ByteBuffer

final case class StorePointer(poolIndex: Byte, data: Array[Byte]) {
  
  override def toString(): String = {
    if (data.length == 0)
      s"($poolIndex,)"
    else if(data.length == 1)
      s"($poolIndex,${ByteBuffer.wrap(data).get()})"
    else if(data.length == 2)
      s"($poolIndex,${ByteBuffer.wrap(data).getShort()})"
    else if(data.length == 4)
      s"($poolIndex,${ByteBuffer.wrap(data).getInt()})"
    else 
      super.toString()
  }
  
  override def equals(other: Any): Boolean = other match {
    case rhs: StorePointer => poolIndex == rhs.poolIndex && java.util.Arrays.equals(data, rhs.data)
    case _ => false
  }
  
  override def hashCode(): Int = java.util.Arrays.hashCode(data) ^ poolIndex
}