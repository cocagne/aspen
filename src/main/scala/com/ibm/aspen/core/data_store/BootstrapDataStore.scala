package com.ibm.aspen.core.data_store

import scala.concurrent.Future
import java.util.UUID
import java.nio.ByteBuffer
import com.ibm.aspen.core.objects.StorePointer
import com.ibm.aspen.core.objects.ObjectPointer
import com.ibm.aspen.core.DataBuffer
import com.ibm.aspen.core.HLCTimestamp

trait BootstrapDataStore extends DataStore {
  
  /** Defines the Storage Pool this store belongs to and the Index of this store within the pool */
  def storeId: DataStoreID
  
  def maximumAllowedObjectSize: Option[Int] = None
  
  /** Allocates a new Object on the store */
  def bootstrapAllocateNewObject(objectUUID: UUID, initialContent: DataBuffer, timestamp: HLCTimestamp): Future[StorePointer]
  
  /** Overwrites the object content. Future is to data at rest on disk */
  def bootstrapOverwriteObject(objectPointer: ObjectPointer, newContent: DataBuffer, timestamp: HLCTimestamp): Future[Unit]
  
}