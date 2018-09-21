package com.ibm.aspen.core.data_store

import java.util.UUID

import com.ibm.aspen.core.DataBuffer

class MutableDataObject(objectId: StoreObjectID,
                        initialOperation: UUID,
                        loader: MutableObjectLoader,
                        allocationState: Option[(ObjectMetadata, DataBuffer)]) extends MutableObject(objectId,
  initialOperation, loader, allocationState.isDefined) {

  private var db: DataBuffer = allocationState.map(t => t._2).getOrElse(DataBuffer.Empty)

  allocationState.foreach { t =>
    meta = t._1
  }

  def data: DataBuffer = synchronized(db)

  /** Called when state is loaded from the store */
  protected def stateLoadedFromBackingStore(m: ObjectMetadata, odb: Option[DataBuffer]): Unit = synchronized {
    this.meta = m
    odb.foreach(db => this.db = db)
  }
  
  def overwriteData(db: DataBuffer): Unit = synchronized { this.db = db }
  
  def appendData(db: DataBuffer): Unit = synchronized { this.db = this.db.append(db) }
  
  def restore(meta: ObjectMetadata, data: DataBuffer): Unit = synchronized {
    this.meta = meta
    this.db = data
  }
  
}
