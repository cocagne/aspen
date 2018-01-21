package com.ibm.aspen.core.data_store

import java.util.UUID
import scala.concurrent.Future
import com.ibm.aspen.core.objects.StorePointer
import com.ibm.aspen.core.DataBuffer

class MutableObjectLoader(val backend: DataStoreBackend) {
  
  implicit val executionContext = backend.executionContext
  
  protected var objects = Map[UUID, MutableObject]()
  protected var loadingFutures = Map[UUID, Future[Either[ObjectReadError, MutableObject]]]()
  
  def createNewObject(
      objectUUID: UUID,
      allocationTransactionUUID: UUID,
      storePointer: StorePointer, 
      metadata: ObjectMetadata, 
      data: DataBuffer): MutableObject = synchronized {
    val obj = new MutableObject(StoreObjectID(objectUUID, storePointer), allocationTransactionUUID, this)
    obj.metadata = metadata
    obj.data = data
    objects += (objectUUID -> obj)
    obj
  }
  
  def getAlreadyLoadedObject(objectUUID: UUID): Option[MutableObject] = synchronized { objects.get(objectUUID) }
  
  /** Immediately returns a MutableObject to represent the requested object. 
   * 
   */
  def load(objectId: StoreObjectID, operation: UUID): MutableObject = synchronized {
    objects.get(objectId.objectUUID) match {
      case Some(obj) =>
        obj.beginOperation(operation)
        obj
        
      case None =>
        val obj = new MutableObject(objectId, operation, this)
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