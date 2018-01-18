package com.ibm.aspen.core.objects.keyvalue

import com.ibm.aspen.core.HLCTimestamp

final case class KVState(key: Array[Byte], value: Array[Byte], timestamp: HLCTimestamp) {
  override def equals(other: Any): Boolean = other match {
    case that: KVState => java.util.Arrays.equals(key, that.key) && java.util.Arrays.equals(value, that.value) && timestamp.asLong == that.timestamp.asLong
    case _ => false
  }
  override def hashCode: Int = key.hashCode() ^ value.hashCode ^ timestamp.hashCode()
}