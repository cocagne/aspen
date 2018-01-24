package com.ibm.aspen.core.data_store

import scala.concurrent.ExecutionContext
import scala.concurrent.Future
import java.util.UUID
import com.ibm.aspen.core.DataBuffer
import com.ibm.aspen.core.allocation.AllocationErrors
import com.ibm.aspen.core.objects.StorePointer

object MemoryOnlyDataStoreBackend {
  class Obj(var metadata: ObjectMetadata, var data: DataBuffer)
  
  val NullArray =new Array[Byte](0)
}

class MemoryOnlyDataStoreBackend(
    implicit override val executionContext: ExecutionContext) extends DataStoreBackend {
  
  import MemoryOnlyDataStoreBackend._
  
  var objects = Map[UUID, Obj]()  
  
  override def close(): Future[Unit] = Future.successful(())
  
  override def haveFreeSpaceForOverwrite(objectId: StoreObjectID, currentDataSize: Int, newDataSize: Int): Boolean = true
  
  override def haveFreeSpaceForAppend(objectId: StoreObjectID, currentDataSize: Int, newDataSize: Int): Boolean = true
  
  override def allocateObject(objectUUID: UUID, metadata: ObjectMetadata, data: DataBuffer): Future[Either[AllocationErrors.Value, Array[Byte]]] = {
    val o = new Obj(metadata, data)
    objects += (objectUUID -> o)
    Future.successful(Right(NullArray))
  }
  
  override def deleteObject(objectId: StoreObjectID): Future[Unit] = {
    objects -= objectId.objectUUID
    Future.successful(())
  }
  
  override def getObjectMetaData(objectId: StoreObjectID): Future[Either[ObjectReadError, ObjectMetadata]] = Future.successful {
    objects.get(objectId.objectUUID) match {
      case None => Left(new InvalidLocalPointer)
      case Some(o) => Right(o.metadata)
    }
  }
  
  override def getObjectData(objectId: StoreObjectID): Future[Either[ObjectReadError, DataBuffer]] = Future.successful {
    objects.get(objectId.objectUUID) match {
      case None => Left(new InvalidLocalPointer)
      case Some(o) => Right(o.data)
    }
  }
  
  override def getObject(objectId: StoreObjectID): Future[Either[ObjectReadError, (ObjectMetadata, DataBuffer)]] = Future.successful {
    objects.get(objectId.objectUUID) match {
      case None => Left(new InvalidLocalPointer)
      case Some(o) => Right((o.metadata, o.data))
    }
  }
  
  override def putObjectMetaData(objectId: StoreObjectID, metadata: ObjectMetadata): Future[Unit] = Future.successful {
    objects.get(objectId.objectUUID) match {
      case None => assert(false, "Put attempted on non-existent object")
      case Some(o) => o.metadata = metadata
    }
  }
  
  override def putObjectData(objectId: StoreObjectID, data:DataBuffer): Future[Unit] = Future.successful {
    objects.get(objectId.objectUUID) match {
      case None => assert(false, "Put attempted on non-existent object")
      case Some(o) => o.data = data
    }
  }
  
  override def putObject(objectId: StoreObjectID, metadata: ObjectMetadata, data: DataBuffer): Future[Unit] = Future.successful {
    objects.get(objectId.objectUUID) match {
      case None => assert(false, "Put attempted on non-existent object")
      case Some(o) => 
        o.metadata = metadata
        o.data = data
    }
  }
}