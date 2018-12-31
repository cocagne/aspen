package com.ibm.aspen

import java.nio.ByteBuffer
import java.util.UUID
import java.nio.charset.StandardCharsets

package object amorfs {
  
  def string2arr(s: String): Array[Byte] = s.getBytes(StandardCharsets.UTF_8)
  
  def arr2string(arr: Array[Byte]): String = new String(arr, StandardCharsets.UTF_8)
  
  def int2arr(i: Int): Array[Byte] = {
    val arr = new Array[Byte](4)
    val bb = ByteBuffer.wrap(arr)
    bb.putInt(i)
    arr
  }
  def arr2int(arr: Array[Byte]): Int = {
    ByteBuffer.wrap(arr).getInt()
  }
  
  def long2arr(i: Long): Array[Byte] = {
    val arr = new Array[Byte](8)
    val bb = ByteBuffer.wrap(arr)
    bb.putLong(i)
    arr
  }
  def arr2long(arr: Array[Byte]): Long = {
    ByteBuffer.wrap(arr).getLong()
  }
  
  def encodeIntArray(arr: Array[Int]): Array[Byte] = {
    val barr = new Array[Byte](4*arr.length)
    val bb = ByteBuffer.wrap(barr)
    arr.foreach(i => bb.putInt(i))
    barr
  }
  def decodeIntArray(arr: Array[Byte]): Array[Int] = {
    require(arr.length % 4 == 0)
    val iarr = new Array[Int](arr.length/4)
    val bb = ByteBuffer.wrap(arr)
    for (i <- 0 until iarr.length)
      iarr(i) = bb.getInt()
    iarr
  }
  
  def encodeUUIDArray(arr: Array[UUID]): Array[Byte] = {
    val barr = new Array[Byte](16*arr.length)
    val bb = ByteBuffer.wrap(barr)
    arr.foreach { uuid => {
      bb.putLong(uuid.getMostSignificantBits)
      bb.putLong(uuid.getLeastSignificantBits)
    }}
    barr
  }
  def decodeUUIDArray(arr: Array[Byte]): Array[UUID] = {
    require(arr.length % 16 == 0)
    val uarr = new Array[UUID](arr.length/16)
    val bb = ByteBuffer.wrap(arr)
    for (i <- 0 until uarr.length) {
      val msb = bb.getLong()
      val lsb = bb.getLong()
      uarr(i) = new UUID(msb, lsb)
    }
    uarr
  }
}