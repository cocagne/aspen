package com.ibm.aspen.core.objects

final case class StorePointer(poolIndex: Byte, data: Array[Byte]) {
  override def equals(other: Any): Boolean = other match {
    case rhs: StorePointer => poolIndex == rhs.poolIndex && java.util.Arrays.equals(data, rhs.data)
    case _ => false
  }
}