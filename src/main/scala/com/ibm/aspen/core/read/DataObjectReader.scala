package com.ibm.aspen.core.read

import com.ibm.aspen.core.data_store.DataStoreID
import com.ibm.aspen.core.objects._
import com.ibm.aspen.core.{DataBuffer, HLCTimestamp}

class DataObjectReader(metadataOnly: Boolean, pointer: DataObjectPointer, reread: DataStoreID => Unit)
  extends BaseObjectReader[DataObjectPointer, DataObjectStoreState](metadataOnly, pointer, reread) {

  override protected def createObjectState(storeId:DataStoreID, readTime: HLCTimestamp, cs: ReadResponse.CurrentState): DataObjectStoreState = {
    new DataObjectStoreState(storeId, cs.revision, cs.refcount, cs.timestamp, readTime, cs.sizeOnStore, cs.objectData)
  }

  override protected def restoreObject(revision:ObjectRevision, refcount: ObjectRefcount, timestamp:HLCTimestamp,
                                       readTime: HLCTimestamp, storeStates: List[DataObjectStoreState]): Unit = {

    val sizeOnStore = storeStates.head.sizeOnStore

    val segments = storeStates.foldLeft(List[(Byte,DataBuffer)]()) { (l, ss) => ss.objectData match {
      case None => l
      case Some(db) => (ss.storeId.poolIndex -> db) :: l
    }}

    if (segments.size >= threshold) {
      val data = pointer.ida.restore(segments)
      val obj = DataObjectState(pointer, revision, refcount, timestamp, readTime, sizeOnStore, data)
      endResult = Some(Right(obj))
    }
  }
}