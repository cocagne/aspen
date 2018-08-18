package com.ibm.aspen.core.data_store

import java.util.UUID
import com.ibm.aspen.core.DataBuffer

class MutableDataObject( 
    objectId: StoreObjectID, 
    initialOperation: UUID, 
    loader: MutableObjectLoader,
    ostate: Option[(ObjectMetadata, DataBuffer)]) extends MutableObject(objectId, initialOperation, loader, ostate) {
  
  def overwriteData(db: DataBuffer): Unit = dataBuffer = db
  
  def appendData(db: DataBuffer): Unit = dataBuffer = dataBuffer.append(db)
  
  def restore(meta: ObjectMetadata, data: DataBuffer): Unit = setRebuildState(meta, data)
  
}
