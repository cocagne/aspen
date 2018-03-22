package com.ibm.aspen

import java.nio.ByteBuffer

package object cumulofs {
    
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
}