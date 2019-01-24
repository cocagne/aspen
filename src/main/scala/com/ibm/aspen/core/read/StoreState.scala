package com.ibm.aspen.core.read

import java.util.UUID

import com.ibm.aspen.core.{DataBuffer, HLCTimestamp}
import com.ibm.aspen.core.data_store.{DataStoreID, StoreKeyValueObjectContent}
import com.ibm.aspen.core.objects.{ObjectRefcount, ObjectRevision}


sealed abstract class StoreState(
                                  val storeId: DataStoreID,
                                  val revision: ObjectRevision,
                                  val refcount: ObjectRefcount,
                                  val timestamp: HLCTimestamp,
                                  val readTimestamp: HLCTimestamp) {

  var rereadRequired: Boolean = false

  def lastUpdateTimestamp: HLCTimestamp

  def debugLogStatus(log: String => Unit): Unit
}

class DataObjectStoreState(
                            storeId: DataStoreID,
                            revision: ObjectRevision,
                            refcount: ObjectRefcount,
                            timestamp: HLCTimestamp,
                            readTimestamp: HLCTimestamp,
                            val sizeOnStore: Int,
                            val objectData: Option[DataBuffer]) extends StoreState(storeId, revision, refcount, timestamp, readTimestamp) {

  def lastUpdateTimestamp: HLCTimestamp = timestamp

  def debugLogStatus(log: String => Unit): Unit = log(s"  DOSS ${storeId.poolIndex} Rev $revision Ref $refcount $timestamp")

}

class KeyValueObjectStoreState(
                                storeId: DataStoreID,
                                revision: ObjectRevision,
                                refcount: ObjectRefcount,
                                timestamp: HLCTimestamp,
                                readTimestamp: HLCTimestamp,
                                objectData: Option[DataBuffer],
                                val lockedWriteTransactions: Set[UUID]) extends StoreState(storeId, revision, refcount, timestamp, readTimestamp) {

  val kvoss: StoreKeyValueObjectContent = objectData match {
    case None => new StoreKeyValueObjectContent(None, None, None, None, Map())
    case Some(db) => StoreKeyValueObjectContent(db)
  }

  def lastUpdateTimestamp: HLCTimestamp = kvoss.lastUpdateTimestamp

  def debugLogStatus(log: String => Unit): Unit = {
    log(s"  KVOSS ${storeId.poolIndex} Rev $revision Ref $refcount TS $timestamp")
    kvoss.debugLogStatus(log)
  }
}