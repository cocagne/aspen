package com.ibm.aspen.core.data_store

import scala.concurrent.ExecutionContext
import com.ibm.aspen.core.objects.ObjectPointer
import scala.concurrent.Future
import com.ibm.aspen.core.DataBuffer
import java.util.UUID
import com.ibm.aspen.core.HLCTimestamp
import com.ibm.aspen.core.allocation.AllocationErrors

/** Interface DataStore Backends must provide.
 *  
 *  All of these methods are implicitly synchronized by virtue of the wrapping DataStoreFrontend only calling these
 *  methods from within synchronized blocks. These methods will never be called concurrently.
 * 
 */
trait DataStoreBackend {
  
  /** This execution context will be used by the enclosing DataStoreFrontend for executing callbacks
   */
  implicit val executionContext: ExecutionContext
  
  /** Shuts down the store and releases all runtime resources
   */
  def close(): Future[Unit]
  
  /** Allocates an object on the store. When the future returns, the data does not need to have been written to the store.
   *  The returned array will go in to the StorePointer of the resulting ObjectPointer and is provided for all future
   *  gets and puts via the objectId.storePointer.data array.
   * 
   */
  def allocateObject(objectUUID: UUID, metadata: ObjectMetadata, data: DataBuffer): Future[Either[AllocationErrors.Value, Array[Byte]]]
  
  def deleteObject(objectId: StoreObjectID): Future[Unit]
  
  def getObjectMetaData(objectId: StoreObjectID): Future[Either[ObjectReadError, ObjectMetadata]]
  
  def getObjectData(objectId: StoreObjectID): Future[Either[ObjectReadError, DataBuffer]]
  
  def getObject(objectId: StoreObjectID): Future[Either[ObjectReadError, (ObjectMetadata, DataBuffer)]]
  
  def putObjectMetaData(objectId: StoreObjectID, metadata: ObjectMetadata): Future[Unit]
  
  def putObjectData(objectId: StoreObjectID, data:DataBuffer): Future[Unit]
  
  def putObject(objectId: StoreObjectID, metadata: ObjectMetadata, data: DataBuffer): Future[Unit]
}