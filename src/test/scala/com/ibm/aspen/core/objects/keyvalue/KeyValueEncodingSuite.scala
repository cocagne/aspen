package com.ibm.aspen.core.objects.keyvalue

import org.scalatest._
import java.util.UUID
import com.ibm.aspen.core.ida.Replication
import com.ibm.aspen.core.data_store.DataStoreID
import com.ibm.aspen.core.objects.StorePointer
import com.ibm.aspen.core.objects.KeyValueObjectPointer
import com.ibm.aspen.core.objects.ObjectRevision
import com.ibm.aspen.core.objects.ObjectRefcount
import com.ibm.aspen.core.HLCTimestamp
import com.ibm.aspen.core.objects.KeyValueObjectState
import java.nio.ByteBuffer
import com.ibm.aspen.core.DataBuffer

object KeyValueEncodingSuite {
  val objUUID = new UUID(0,1)
  val poolUUID = new UUID(0,2)
  
  val ida = Replication(3,2)
  
  val ds0 = DataStoreID(poolUUID, 0)
  val ds1 = DataStoreID(poolUUID, 1)
  val ds2 = DataStoreID(poolUUID, 2)
  
  val sp0 = StorePointer(0, List[Byte](0).toArray)
  val sp1 = StorePointer(1, List[Byte](1).toArray)
  val sp2 = StorePointer(2, List[Byte](2).toArray)
  
  val rev = ObjectRevision(new UUID(0,1))
  val ref = ObjectRefcount(0,1)
  val ts = HLCTimestamp(5)
  
  val ptr = KeyValueObjectPointer(objUUID, poolUUID, None, ida, (sp0 :: sp1 :: sp2 :: Nil).toArray)
  
  val d0 = List[Byte](1,2,3).toArray
  val d1 = List[Byte](4,5,6).toArray
  val d2 = List[Byte](7,8,9).toArray
  val d3 = List[Byte](0,0,0).toArray
  
  val sz = 5
}

class KeyValueEncodingSuite extends FunSuite with Matchers {
  import KeyValueEncodingSuite._
  
  def encdec(kvos: KeyValueObjectState): KeyValueObjectState = {
    val enc = KeyValueObjectCodec.encode(ida, kvos)
    
    val storeStates = enc.zipWithIndex.map( t => KeyValueObjectStoreState(t._2.asInstanceOf[Byte], t._1) ).toList
    
    KeyValueObjectCodec.decode(ptr, rev, ref, ts, sz, storeStates)
  }
  
  test("Simple Encoding Empty State") {
    
    val kvos = new KeyValueObjectState(ptr, rev, ref, ts, sz, None, None, None, None, Map())
    
    encdec(kvos) should be (kvos)
  }
  
  test("Simple Encoding Optional State") {
    
    val kvos = new KeyValueObjectState(ptr, rev, ref, ts, sz, Some(Key(d0)), Some(Key(d1)), Some(d2), Some(d3), Map())

    encdec(kvos) should be (kvos)
  }
  
  test("Single KeyValue pair") {
    val k0 = Key(d0)
    val v0 = Value(k0, d1, ts)
   
    val kvos = new KeyValueObjectState(ptr, rev, ref, ts, sz, None, None, None, None, Map( (k0->v0) ))
    
    encdec(kvos) should be (kvos)
  }
  
  test("Multi KeyValue pair") {
    val k0 = Key(d0)
    val v0 = Value(k0, d1, ts)
    
    val k1 = Key(d1)
    val v1 = Value(k1, d2, ts)
    
    val kvos = new KeyValueObjectState(ptr, rev, ref, ts, sz, None, None, None, None, Map( (k1->v1), (k0->v0) ))
    
    encdec(kvos) should be (kvos)
  }
  
  test("Multiple Updates") {
    val k0 = Key(d0)
    val v0 = Value(k0, d1, ts)
    
    val k1 = Key(d1)
    val v1 = Value(k1, d2, ts)
    
    val k2 = Key(d3)
    val v2 = Value(k2, d3, ts)
    
    val kvos = new KeyValueObjectState(ptr, rev, ref, ts, sz, None, None, None, None, Map( (k1->v1), (k2->v2) ))
        
    val ops: List[KeyValueOperation] = List(new Delete(d1), new Insert(d0, d1, ts), new SetLeft(d3))
    
    val orig = KeyValueObjectCodec.encode(ida, kvos)
    val updates = KeyValueObjectCodec.encodeUpdate(ida, ops)
    
    val conjoined = orig.zip(updates).map { t => 
      val bb = ByteBuffer.allocate(t._1.size + t._2.size)
      bb.put(t._1)
      bb.put(t._2)
      bb.position(0)
      DataBuffer(bb)
    }
    
    val storeStates = conjoined.zipWithIndex.map( t => KeyValueObjectStoreState(t._2.asInstanceOf[Byte], t._1) ).toList
    
    val dec = KeyValueObjectCodec.decode(ptr, rev, ref, ts, sz, storeStates)
    
    val expected = new KeyValueObjectState(ptr, rev, ref, ts, sz, None, None, Some(d3), None, Map( (k0->v0), (k2->v2) ))
    
    dec should be (expected)
  }
  
}