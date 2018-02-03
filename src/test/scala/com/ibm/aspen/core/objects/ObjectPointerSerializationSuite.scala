package com.ibm.aspen.core.objects

import org.scalatest._
import java.util.UUID
import com.ibm.aspen.core.ida.Replication

class ObjectPointerSerializationSuite  extends FunSuite with Matchers {
  
  test("Full Serialization") {
    val objUUID = new UUID(3,4)
    val poolUUID = new UUID(5,6)
    val size = Some(10)
    val p1 = StorePointer(0, Array[Byte](0, 1, 2, 3))
    val p2 = StorePointer(1, Array[Byte](3, 2, 1, 0))
    val p3 = StorePointer(2, Array[Byte](0, 1, 1, 0))
    val pointers = (p1::p2::p3::Nil).toArray
    val ida = Replication(3,2)
    
    val op = DataObjectPointer(objUUID, poolUUID, size, ida, pointers)
    
    val arr = op.toArray
    
    val fbArr = com.ibm.aspen.core.network.NetworkCodec.objectPointerToByteArray(op)
    
    //println(s"Encoded size: ${arr.length} fb size: ${fbArr.length}")
    
    val op2 = ObjectPointer.fromArray(arr)
    
    op2 should be (op)
  }
  
  test("No local pointer arrays") {
    val objUUID = new UUID(3,4)
    val poolUUID = new UUID(5,6)
    val size = Some(10)
    val p1 = StorePointer(0, new Array[Byte](0))
    val p2 = StorePointer(1, new Array[Byte](0))
    val p3 = StorePointer(2, new Array[Byte](0))
    val pointers = (p1::p2::p3::Nil).toArray
    val ida = Replication(3,2)
    
    val op = DataObjectPointer(objUUID, poolUUID, size, ida, pointers)
    
    val arr = op.toArray
    
    val fbArr = com.ibm.aspen.core.network.NetworkCodec.objectPointerToByteArray(op)
    
    //println(s"Encoded size: ${arr.length} fb size: ${fbArr.length}")
    
    val op2 = ObjectPointer.fromArray(arr)
    
    op2 should be (op)
  }
  
  test("No local pointer arrays, no size") {
    val objUUID = new UUID(3,4)
    val poolUUID = new UUID(5,6)
    val size =None
    val p1 = StorePointer(0, new Array[Byte](0))
    val p2 = StorePointer(1, new Array[Byte](0))
    val p3 = StorePointer(2, new Array[Byte](0))
    val pointers = (p1::p2::p3::Nil).toArray
    val ida = Replication(3,2)
    
    val op = DataObjectPointer(objUUID, poolUUID, size, ida, pointers)
    
    val arr = op.toArray
    
    val fbArr = com.ibm.aspen.core.network.NetworkCodec.objectPointerToByteArray(op)
    
    //println(s"Encoded size: ${arr.length} fb size: ${fbArr.length}")
    
    val op2 = ObjectPointer.fromArray(arr)
    
    op2 should be (op)
  }
}