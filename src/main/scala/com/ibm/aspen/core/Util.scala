package com.ibm.aspen.core

import java.nio.ByteBuffer
import java.util.UUID
import java.io.StringWriter
import java.io.PrintWriter

object Util {
  
  def uuid2byte(uuid: UUID): Array[Byte] = {
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
  
}