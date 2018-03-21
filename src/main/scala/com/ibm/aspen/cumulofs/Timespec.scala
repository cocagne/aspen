package com.ibm.aspen.cumulofs

import java.nio.ByteBuffer

case class Timespec(seconds: Long, nanoseconds: Long) {
  def toArray(): Array[Byte] = {
    val arr = new Array[Byte](16)
    val bb = ByteBuffer.wrap(arr)
    bb.putLong(seconds)
    bb.putLong(nanoseconds)
    arr
  }
}

object Timespec {
  def apply(arr: Array[Byte]): Timespec = {
    val bb = ByteBuffer.wrap(arr)
    val sec = bb.getLong()
    val nsec = bb.getLong()
    new Timespec(sec, nsec)
  }
  
  def now: Timespec = {
    val ts = System.currentTimeMillis()
    val sec = ts / 1000
    val nsec = (ts - sec) * 1000
    Timespec(sec, nsec)
  }
}