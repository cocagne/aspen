package com.ibm.aspen.core.data_store

import scala.concurrent.ExecutionContext
import com.ibm.aspen.core.objects.ObjectPointer
import scala.concurrent.Future
import com.ibm.aspen.core.DataBuffer
import com.ibm.aspen.core.objects.StorePointer
import java.util.UUID
import com.ibm.aspen.core.HLCTimestamp
import com.ibm.aspen.core.allocation.AllocationErrors

trait DataStoreBackend {
  
  /** This execution context will be used by the enclosing DataStoreFrontend for executing callbacks
   */
  implicit val executionContext: ExecutionContext
  
  /** Shuts down the store and releases all runtime resources
   */
  def close(): Future[Unit]
  
  def allocateObject(objectUUID: UUID, metadata: ObjectMetadata, data: DataBuffer): Future[Either[AllocationErrors.Value, StorePointer]]
  
  def deleteObject(objectId: StoreObjectID): Future[Unit]
  
  def getObjectMetaData(objectId: StoreObjectID): Future[Either[ObjectReadError, ObjectMetadata]]
  
  def getObjectData(objectId: StoreObjectID): Future[Either[ObjectReadError, DataBuffer]]
  
  def getObject(objectId: StoreObjectID): Future[Either[ObjectReadError, (ObjectMetadata, DataBuffer)]]
  
  def putObjectMetaData(objectId: StoreObjectID, metadata: ObjectMetadata): Future[Unit]
  
  def putObjectData(objectId: StoreObjectID, data:DataBuffer): Future[Unit]
  
  def putObject(objectId: StoreObjectID, metadata: ObjectMetadata, data: DataBuffer): Future[Unit]
}