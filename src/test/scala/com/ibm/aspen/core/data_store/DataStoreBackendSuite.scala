package com.ibm.aspen.core.data_store

import java.util.UUID

import com.ibm.aspen.base.impl.TempDirSuiteBase
import com.ibm.aspen.core.{DataBuffer, HLCTimestamp}
import com.ibm.aspen.core.objects.{ObjectRefcount, ObjectRevision, StorePointer}

import scala.concurrent._
import scala.concurrent.duration._

object DataStoreBackendSuite {
  val awaitDuration = Duration(100, MILLISECONDS)
  
  def await[T](f: Future[T]): T = Await.result(f, awaitDuration)
  
  val d1 = DataBuffer(List[Byte](1,2,3).toArray)
  val d2 = DataBuffer(List[Byte](4,5,6).toArray)
  
  val u1 = new UUID(1,1)
  val u2 = new UUID(2,2)
  val m1 = ObjectMetadata(ObjectRevision(u1), ObjectRefcount(0,1), HLCTimestamp(5))
  val m2 = ObjectMetadata(ObjectRevision(u2), ObjectRefcount(0,1), HLCTimestamp(10))
}

abstract class DataStoreBackendSuite extends TempDirSuiteBase {
  
  import DataStoreBackendSuite._
  
  // To be initialized in preTest()
  var backend: DataStoreBackend = _
  
  override def preTempDirDeletion(): Unit = { await(backend.close()) }
  
  test("Alloc get put delete") {
    val arr = await(backend.allocateObject(u1, m1, d1)) match {
      case Left(_) => fail("should not error")
      case Right(x) => x
    }
    val o1 = StoreObjectID(u1, StorePointer(0,arr))
    
    await(backend.getObjectMetaData(o1)) should be (Right(m1))

    await(backend.getObject(o1)) should be (Right((m1, d1)))
    
    await(backend.putObjectMetaData(o1, m2))
    
    await(backend.getObjectMetaData(o1)) should be (Right(m2))

    await(backend.getObject(o1)) should be (Right((m2, d1)))
    

    await(backend.putObject(o1, m1, d1))
    
    await(backend.getObjectMetaData(o1)) should be (Right(m1))
    await(backend.getObject(o1)) should be (Right((m1, d1)))
    
    await(backend.deleteObject(o1))
    
    await(backend.getObjectMetaData(o1)) should be (Left(new InvalidLocalPointer))
    await(backend.getObject(o1)) should be (Left(new InvalidLocalPointer))
  }
  
  test("Fail on read of invalid object") {
    
    val o1 = StoreObjectID(u1, StorePointer(0,new Array[Byte](0)))
    
    await(backend.getObjectMetaData(o1)) should be (Left(new InvalidLocalPointer))
    await(backend.getObject(o1)) should be (Left(new InvalidLocalPointer))
  }
}