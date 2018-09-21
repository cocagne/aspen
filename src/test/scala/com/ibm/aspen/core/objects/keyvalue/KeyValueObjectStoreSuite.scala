package com.ibm.aspen.core.objects.keyvalue

import java.util.UUID

import com.ibm.aspen.core.HLCTimestamp
import com.ibm.aspen.core.data_store.{DataStoreID, StoreKeyValueObjectContent}
import com.ibm.aspen.core.ida.Replication
import com.ibm.aspen.core.objects.{KeyValueObjectPointer, ObjectRefcount, ObjectRevision, StorePointer}
import org.scalatest._

object KeyValueObjectStoreSuite {
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

class KeyValueObjectStoreSuite extends FunSuite with Matchers {
  import KeyValueObjectStoreSuite._
  
  val ts = HLCTimestamp.now
  val rev = ObjectRevision(new UUID(1,2))
  
  def encdec(ops: List[KeyValueOperation], okvoss: Option[StoreKeyValueObjectContent]=None): StoreKeyValueObjectContent = {
    val kvoss = okvoss.getOrElse(StoreKeyValueObjectContent())
    val enc = KeyValueOperation.encode(ops, ida)
    kvoss.update(enc(0), rev, ts)
  }
  
//  test("Simple Encoding Empty State") {
//    encdec(Nil) should be (KeyValueObjectStoreState())
//  }
  
  
  
}