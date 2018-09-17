package com.ibm.aspen

import java.util.UUID
import java.nio.ByteBuffer
import java.io.StringWriter
import java.io.PrintWriter
import com.ibm.aspen.core.DataBuffer

package object util {
  import scala.language.implicitConversions
  
  implicit def uuid2byte(uuid: UUID): Array[Byte] = {
    val bb = ByteBuffer.allocate(16)
    bb.putLong(0, uuid.getMostSignificantBits)
    bb.putLong(8, uuid.getLeastSignificantBits)
    bb.array()
  }
  
  def byte2uuid(arr: Array[Byte]): UUID = {
    val bb = ByteBuffer.wrap(arr)
    val msb = bb.getLong()
    val lsb = bb.getLong()
    new UUID(msb, lsb)
  }
  
  def int2byte(i: Int): Array[Byte] = {
    val arr = new Array[Byte](4)
    val bb = ByteBuffer.wrap(arr)
    bb.putInt(i)
    arr
  }
  def byte2int(arr: Array[Byte]): Int = ByteBuffer.wrap(arr).getInt()
  
  def getStack(): String = {
    val e = new Exception("printing stack")
    val sw = new StringWriter()
    val pw = new PrintWriter(sw)
    e.printStackTrace(pw)
    sw.toString()
  }
  
  def printStack(): Unit = println(getStack())
  
  def db2string(db: DataBuffer): String = {
    val enc = java.util.Base64.getEncoder()
    enc.encodeToString(db.getByteArray())
  }
  def printableArray(arr: Array[Byte]): String = {
    val enc = java.util.Base64.getEncoder()
    enc.encodeToString(arr)
  }
}