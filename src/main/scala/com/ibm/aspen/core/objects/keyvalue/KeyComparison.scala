package com.ibm.aspen.core.objects.keyvalue

sealed abstract class KeyComparison {
  def apply(a: Key, b: Key): Int
  def apply(a: Key, b: Array[Byte]): Int = apply(a, Key(b))
  def apply(a: Array[Byte], b: Key): Int = apply(Key(a), b)
  def apply(a: Array[Byte], b: Array[Byte]): Int = apply(Key(a), Key(b))
}

class ByteArrayComparison extends KeyComparison {
  override def apply(a: Key, b: Key): Int = {
    for (i <- 0 until a.bytes.length) {
      if (i > b.bytes.length) return 1 // a is longer than b and all preceeding bytes are equal
      if (a.bytes(i) < b.bytes(i)) return -1 // a is less than b
      if (a.bytes(i) > b.bytes(i)) return 1  // a is greater than b
    }
    if (b.bytes.length > a.bytes.length) return -1 // b is longer than a and all preceeding bytes are equal
    0 // a and b are the same length and have matching content
  }
}

class IntegerComparison extends KeyComparison {
  override def apply(a: Key, b: Key): Int = {
    if (a.bytes.length == 0 && b.bytes.length == 0)
      0
    else if (a.bytes.length == 0 && b.bytes.length != 0)
      -1
    else if (a.bytes.length != 0 && b.bytes.length == 0)
      1
    else {
      val bigA = new java.math.BigInteger(a.bytes)
      val bigB = new java.math.BigInteger(b.bytes)
      bigA.compareTo(bigB)
    }
  }
}

class LexicalComparison extends KeyComparison {
  override def apply(a: Key, b: Key): Int = {
    val sa = new String(a.bytes, "UTF-8")
    val sb = new String(b.bytes, "UTF-8")
    sa.compareTo(sb)
  }
}