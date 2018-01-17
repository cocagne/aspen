package com.ibm.aspen.util

import org.scalatest._
import java.nio.ByteBuffer

class VartintSuite extends FunSuite with Matchers {
 test("Encode unsigned small int") {
   val i = 5
   val bb = ByteBuffer.allocate(10)
   Varint.putUnsignedInt(bb, i)
   bb.position should be (1)
   bb.position(0)
   Varint.getUnsignedInt(bb) should be (i)
 }
 
 test("Encode negative small int") {
   val i = -16
   val bb = ByteBuffer.allocate(10)
   Varint.putSignedInt(bb, i)
   bb.position should be (1)
   bb.position(0)
   Varint.getSignedInt(bb) should be (i)
 }
 
 test("Encode signed positive small int") {
   val i = 5
   val bb = ByteBuffer.allocate(10)
   Varint.putSignedInt(bb, i)
   bb.position should be (1)
   bb.position(0)
   Varint.getSignedInt(bb) should be (i)
 }
 
 test("Encode unsigned short int") {
   val i = 4096
   val bb = ByteBuffer.allocate(10)
   Varint.putUnsignedInt(bb, i)
   bb.position should be (2)
   bb.position(0)
   Varint.getUnsignedInt(bb) should be (i)
 }
 
 test("Encode signed negative short int") {
   val i = -4096
   val bb = ByteBuffer.allocate(10)
   Varint.putSignedInt(bb, i)
   bb.position should be (2)
   bb.position(0)
   Varint.getSignedInt(bb) should be (i)
 }
 
 test("Encode signed positive long int") {
   val i = 5000000000L
   val bb = ByteBuffer.allocate(10)
   Varint.putSignedLong(bb, i)
   bb.position should be (5)
   bb.position(0)
   Varint.getSignedLong(bb) should be (i)
 }
 
 test("Encode unsigned long int") {
   val i = 5000000000L
   val bb = ByteBuffer.allocate(10)
   Varint.putUnsignedLong(bb, i)
   bb.position should be (5)
   bb.position(0)
   Varint.getUnsignedLong(bb) should be (i)
 }
 
 test("Encode signed negative long int") {
   val i = -5000000000L
   val bb = ByteBuffer.allocate(10)
   Varint.putSignedLong(bb, i)
   bb.position should be (5)
   bb.position(0)
   Varint.getSignedLong(bb) should be (i)
 }
}