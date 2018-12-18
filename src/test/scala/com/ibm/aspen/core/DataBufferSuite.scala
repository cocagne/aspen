package com.ibm.aspen.core

import org.scalatest.{FunSuite, Matchers}

class DataBufferSuite extends FunSuite with Matchers {

  test("Buffer Identity Equivalence") {
    val a = Array[Byte](1,2,3)
    val a1 = Array[Byte](1,2,3)

    val da = DataBuffer(a)
    val da1 = DataBuffer(a1)

    da.hashString should be (da1.hashString)
  }

  test("Buffer Aggregate Hash Equivalence") {
    val a = Array[Byte](1,2,3)
    val b = Array[Byte](4,5,6)
    val c = Array[Byte](1,2,3,4,5,6)

    val da = DataBuffer(a)
    val db = DataBuffer(b)
    val dc = DataBuffer(c)

    da.hashString should not be db.hashString
    db.hashString should not be dc.hashString
    db.hashString should not be dc.hashString

    DataBuffer.hashString(da :: db :: Nil) should be (dc.hashString)
  }
}
