package com.ibm.aspen.core.read

import com.ibm.aspen.core.objects.keyvalue.Key
import com.ibm.aspen.core.objects.keyvalue.KeyComparison

sealed abstract class ReadType

case class MetadataOnly() extends ReadType

case class FullObject() extends ReadType

case class ByteRange(offset: Int, length: Int) extends ReadType

case class SingleKey(key: Key, comparison: KeyComparison) extends ReadType

case class LargestKeyLessThan(key: Key, comparison: KeyComparison) extends ReadType

case class LargestKeyLessThanOrEqualTo(key: Key, comparison: KeyComparison) extends ReadType

case class KeyRange(minimum: Key, maximum: Key, comparison: KeyComparison) extends ReadType