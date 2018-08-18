package com.ibm.aspen.core.objects.keyvalue

import com.ibm.aspen.core.HLCTimestamp
import com.ibm.aspen.core.objects.ObjectRevision

final case class Value(key: Key, value: Array[Byte], timestamp: HLCTimestamp, revision: ObjectRevision) {
  override def equals(other: Any): Boolean = other match {
    case that: Value => key == that.key && java.util.Arrays.equals(value, that.value) && timestamp.asLong == that.timestamp.asLong && revision == that.revision
    case _ => false
  }
  override def hashCode: Int = key.hashCode ^ java.util.Arrays.hashCode(value) ^ timestamp.hashCode() ^ revision.hashCode()
}
