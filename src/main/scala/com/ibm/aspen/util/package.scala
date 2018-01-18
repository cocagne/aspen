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
  
  def printStack(): Unit = {
    val e = new Exception("printing stack")
    val sw = new StringWriter()
    val pw = new PrintWriter(sw)
    e.printStackTrace(pw)
    println(sw.toString())
  }
  
  def db2string(db: DataBuffer): String = {
    val enc = java.util.Base64.getEncoder()
    enc.encodeToString(db.getByteArray())
  }
  def arr2string(arr: Array[Byte]): String = {
    val enc = java.util.Base64.getEncoder()
    enc.encodeToString(arr)
  }
}