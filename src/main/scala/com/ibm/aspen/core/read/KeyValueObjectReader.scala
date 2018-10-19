package com.ibm.aspen.core.read

import java.util.UUID

import com.ibm.aspen.core.HLCTimestamp
import com.ibm.aspen.core.data_store.DataStoreID
import com.ibm.aspen.core.objects.keyvalue.{Key, Value}
import com.ibm.aspen.core.objects.{KeyValueObjectPointer, ObjectRefcount, ObjectRevision}

class KeyValueObjectReader(metadataOnly: Boolean, pointer: KeyValueObjectPointer, reread: DataStoreID => Unit)
  extends ObjectReader[KeyValueObjectPointer, KeyValueObjectStoreState](metadataOnly, pointer, reread) {

  override protected def createObjectState(storeId:DataStoreID, readTime: HLCTimestamp, cs: ReadResponse.CurrentState): KeyValueObjectStoreState = {
    new KeyValueObjectStoreState(storeId, cs.revision, cs.refcount, cs.timestamp, readTime, cs.objectData, cs.lockedWriteTransactions)
  }

  override protected def restoreObject(revision:ObjectRevision, refcount: ObjectRefcount, timestamp:HLCTimestamp,
                                       readTime: HLCTimestamp, storeStates: List[KeyValueObjectStoreState]): Unit = {

    val rereadable = storeStates.filter(_.rereadRequired != true).toSet

    val segments = storeStates.foldLeft(Map[Key, List[(DataStoreID, Value)]]()){ (m, ss) =>
      ss.kvoss.idaEncodedContents.valuesIterator.foldLeft(m){ (subm, v) =>
        val x = subm.get(v.key) match {
          case None => (ss.storeId, v) :: Nil
          case Some(l) => (ss.storeId, v) :: l
        }
        subm + (v.key -> x)
      }
    }

    // Could throw exception on detect non-restorable. Set reread flag first then throw
    // in handler, issue reread calls

  }

  private def anyStoreHasLocked(rev: ObjectRevision, storeStates: List[KeyValueObjectStoreState]): Boolean = {
    storeStates.exists(_.lockedWriteTransactions.contains(rev.lastUpdateTxUUID))
  }

  private def highestRevision(i: Iterator[(HLCTimestamp, ObjectRevision)]): ObjectRevision = {
    val r = i.foldLeft((HLCTimestamp.Zero, ObjectRevision.Null))((h,t) => if (t._1 > h._1) t else h)
    r._2
  }

  private def restoreReplicatedKey(storeStates: List[KeyValueObjectStoreState],
                                   segments: List[(Key, ObjectRevision, HLCTimestamp)]): Option[Key] = {
    if (segments.isEmpty)
      None
    else {
      val rev = highestRevision(segments.iterator.map(t => (t._3, t._2)))
      val matching = filter
    }
  }

}

