package com.ibm.aspen.core.data_store

import java.util.UUID
import scala.concurrent.Future
import com.ibm.aspen.core.objects.StorePointer
import com.ibm.aspen.core.DataBuffer
import com.ibm.aspen.core.objects.ObjectType
import com.ibm.aspen.core.HLCTimestamp
import com.ibm.aspen.core.objects.ObjectRevision

class MutableObjectLoader(val backend: DataStoreBackend) {
  
  implicit val executionContext = backend.executionContext
  
  protected var objects = Map[UUID, MutableObject]()
  protected var loadingFutures = Map[UUID, Future[Either[ObjectReadError, MutableObject]]]()
  
  def createNewObject(
      objectUUID: UUID,
      allocationTransactionUUID: UUID,
      allocationTimestamp: HLCTimestamp,
      storePointer: StorePointer, 
      metadata: ObjectMetadata, 
      data: DataBuffer,
      objectType: ObjectType.Value): MutableObject = synchronized {
    val obj = objectType match {
      case ObjectType.Data => new MutableDataObject(StoreObjectID(objectUUID, storePointer), allocationTransactionUUID, this, Some((metadata, data)))
      case ObjectType.KeyValue => 
        val allocState = (metadata, data, ObjectRevision(allocationTransactionUUID), allocationTimestamp)
        new MutableKeyValueObject(StoreObjectID(objectUUID, storePointer), allocationTransactionUUID, this, Some(allocState))
    }
    objects += (objectUUID -> obj)
    obj
  }
  
  def getAlreadyLoadedObject(objectUUID: UUID): Option[MutableObject] = synchronized { objects.get(objectUUID) }
  
  /** Immediately returns a MutableObject to represent the requested object. 
   * 
   */
  def load(objectId: StoreObjectID, objectType: ObjectType.Value, operation: UUID): MutableObject = synchronized {
    objects.get(objectId.objectUUID) match {
      case Some(obj) =>
        obj.beginOperation(operation)
        obj
        
      case None =>
        val obj = objectType match {
          case ObjectType.Data => new MutableDataObject(objectId, operation, this, None)
          case ObjectType.KeyValue => new MutableKeyValueObject(objectId, operation, this, None)
        }
        objects += (objectId.objectUUID -> obj)
        obj
    }
  }
  
  /** Called when the number of operations referencing an object drops to zero or a ReadError for the object
   *  is encountered.
   */
  protected[data_store] def unload(mobject: MutableObject, error: Option[ObjectReadError]): Unit = synchronized { 
    objects -= mobject.objectId.objectUUID
  }
}