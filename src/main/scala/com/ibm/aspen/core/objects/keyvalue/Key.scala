package com.ibm.aspen.core.objects.keyvalue

import java.util.UUID
import java.nio.ByteBuffer

final case class Key(bytes: Array[Byte]) {
  
  override def equals(other: Any): Boolean = other match {
    case that: Key => java.util.Arrays.equals(bytes, that.bytes)
    case _ => false
  }
  
  override def hashCode: Int = java.util.Arrays.hashCode(bytes)
  
  override def toString(): String = {
    if (bytes.length == 1)
      s"Key(${bytes(0)})"
    else if (bytes.length == 16) {
      val bb = ByteBuffer.wrap(bytes)
      val m = bb.getLong()
      val l = bb.getLong()
      s"Key($m,$l)"
    }
    else
      s"Key(${com.ibm.aspen.util.arr2string(bytes)})"
  }
}

object Key {
  val AbsoluteMinimum = Key(new Array[Byte](0))
  
  import scala.language.implicitConversions
  
  implicit def apply(uuid: UUID): Key = {
    val arr = new Array[Byte](16)
    val bb = ByteBuffer.wrap(arr)
    bb.putLong(uuid.getMostSignificantBits)
    bb.putLong(uuid.getLeastSignificantBits)
    Key(arr)
  }
}