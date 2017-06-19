package com.ibm.aspen.core.data_store

import scala.concurrent._
import ExecutionContext.Implicits.global
import org.scalatest._
import java.util.UUID
import com.ibm.aspen.core.objects.ObjectPointer
import com.ibm.aspen.core.ida.Replication
import com.ibm.aspen.core.objects.StorePointer
import com.ibm.aspen.core.objects.ObjectRefcount
import com.ibm.aspen.core.objects.ObjectRevision

object DataStoreSuite {
  val uuid0 = new UUID(0,0)
  val uuid1 = new UUID(0,1)
  val poolUUID = new UUID(0,2)
  val txUUID = new UUID(0,3)
  
  val allocObj = ObjectPointer(new UUID(0,4), poolUUID, None, Replication(3,2), new Array[StorePointer](0))
  val allocRev = ObjectRevision(0,1)
  val oneRef = ObjectRefcount(0,1)
  
  val storeId = DataStoreID(poolUUID, 1)
}

abstract class DataStoreSuite extends AsyncFunSuite with Matchers {
  import DataStoreSuite._
  
  def newStore: DataStore
  
  test("Allocate New Object") {
    val ds = newStore
    
    /* def allocateNewObject(objectUUID: UUID, 
                        size: Option[Int], 
                        initialContent: Array[Byte],
                        initialRefcount: ObjectRefcount,
                        allocationTransactionUUID: UUID,
                        allocatingObject: ObjectPointer,
                        allocatingObjectRevision: ObjectRevision): Future[Either[ObjectAllocationError.Value, StorePointer]]*/
    
    val icontent = List[Byte](1,2,3).toArray
    val futureResponse = ds.allocateNewObject(uuid0, None, icontent, oneRef, txUUID, allocObj, allocRev)
            
    futureResponse map { either => either match {
      case Right(sp) => sp.poolIndex should be (ds.storeId.poolIndex)
      case Left(err) => fail("Returned failure instead of store pointer")
    }}
	}
  
  test("Allocate and Read New Object") {
    val ds = newStore
    
    val icontent = List[Byte](1,2,3).toArray
    val futureResponse = ds.allocateNewObject(uuid0, None, icontent, oneRef, txUUID, allocObj, allocRev)
            
    val expected = (CurrentObjectState(uuid0, ObjectRevision(0,3), oneRef), icontent)
    
    futureResponse flatMap { either => either match {
      case Right(sp) => ds.getObject(sp).flatMap(er => er match {
        case Right(data) => data should be (expected)
        case Left(err) => fail("Returned failure instead of object content")
      })
      case Left(err) => fail("Returned failure instead of store pointer")
    }}
	}
  
  test("Read Invalid Object") {
    val ds = newStore
    
    val futureResponse = ds.getObject(StorePointer(storeId.poolIndex, new Array[Byte](0)))
    
    futureResponse map { either => either match {
      case Right(_) => fail("Should have failed read")
      case Left(err) => err should be (ObjectError.InvalidLocalPointer)
    }}
	}
  
}