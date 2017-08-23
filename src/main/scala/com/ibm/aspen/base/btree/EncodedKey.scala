package com.ibm.aspen.base.btree

import java.nio.ByteBuffer

trait EncodedKey extends Ordered[EncodedKey] {
  val encoded: ByteBuffer
  def compare(that: EncodedKey) = encoded.compareTo(that.encoded)
}