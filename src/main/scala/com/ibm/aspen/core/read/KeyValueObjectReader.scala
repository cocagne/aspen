package com.ibm.aspen.core.read

import java.util.UUID

import com.ibm.aspen.core.HLCTimestamp
import com.ibm.aspen.core.data_store.DataStoreID
import com.ibm.aspen.core.objects.keyvalue.{Key, Value}
import com.ibm.aspen.core.objects.{KeyValueObjectPointer, KeyValueObjectState, ObjectRefcount, ObjectRevision}


object KeyValueObjectReader {
  private case class NotRestorable() extends Throwable

  private case class Segment[T](revision: ObjectRevision, timestamp: HLCTimestamp, storeID: DataStoreID, data: T)

  private case class Restorable[T](revision: ObjectRevision, timestamp: HLCTimestamp, slices: List[(Byte, T)])
}

class KeyValueObjectReader(metadataOnly: Boolean, pointer: KeyValueObjectPointer, reread: DataStoreID => Unit)
  extends ObjectReader[KeyValueObjectPointer, KeyValueObjectStoreState](metadataOnly, pointer, reread) {

  import KeyValueObjectReader._

  override protected def createObjectState(storeId:DataStoreID, readTime: HLCTimestamp, cs: ReadResponse.CurrentState): KeyValueObjectStoreState = {
    new KeyValueObjectStoreState(storeId, cs.revision, cs.refcount, cs.timestamp, readTime, cs.objectData, cs.lockedWriteTransactions)
  }

  override protected def restoreObject(revision:ObjectRevision, refcount: ObjectRefcount, timestamp:HLCTimestamp,
                                       readTime: HLCTimestamp, storeStates: List[KeyValueObjectStoreState]): Unit = try {

    val min = resolve(storeStates, storeStates.map(ss => ss.kvoss.minimum.map(m => Segment(m.revision, m.timestamp, ss.storeId, m.key))).collect {
      case Some(x) => x
    }).map { r =>
      KeyValueObjectState.Min(r.slices.head._2, r.revision, r.timestamp)
    }

    val max = resolve(storeStates, storeStates.map(ss => ss.kvoss.maximum.map(m => Segment(m.revision, m.timestamp, ss.storeId, m.key))).collect {
      case Some(x) => x
    }).map { r =>
      KeyValueObjectState.Max(r.slices.head._2, r.revision, r.timestamp)
    }

    val left = resolve(storeStates, storeStates.map(ss => ss.kvoss.left.map(m => Segment(m.revision, m.timestamp, ss.storeId, m.idaEncodedContent))).collect {
      case Some(x) => x
    }).map { r =>
      KeyValueObjectState.Left(pointer.ida.restoreArray(r.slices), r.revision, r.timestamp)
    }

    val right = resolve(storeStates, storeStates.map(ss => ss.kvoss.right.map(m => Segment(m.revision, m.timestamp, ss.storeId, m.idaEncodedContent))).collect {
      case Some(x) => x
    }).map { r =>
      KeyValueObjectState.Right(pointer.ida.restoreArray(r.slices), r.revision, r.timestamp)
    }

    val kvrestores = storeStates.foldLeft(Map[Key, List[Segment[Array[Byte]]]]()) { (m, ss) =>
      ss.kvoss.idaEncodedContents.valuesIterator.foldLeft(m) { (subm, v) =>
        val x = subm.get(v.key) match {
          case None => Segment(v.revision, v.timestamp, ss.storeId, v.value) :: Nil
          case Some(l) => Segment(v.revision, v.timestamp, ss.storeId, v.value) :: l
        }
        subm + (v.key -> x)
      }
    }.map(t => t._1 -> resolve(storeStates, t._2))

    val contents = kvrestores.foldLeft(Map[Key,Value]()) { (m, t) => t._2 match {
      case None => m
      case Some(r) => m + (t._1 -> Value(t._1, pointer.ida.restoreArray(r.slices), r.timestamp, r.revision))
    }}

    endResult = Some(Right(new KeyValueObjectState(pointer, revision, refcount, timestamp, readTime, min, max, left, right, contents)))
  }
  catch {
    case _:NotRestorable => ()
  }

  private def anyStoreHasLocked(rev: ObjectRevision, storeStates: List[KeyValueObjectStoreState]): Boolean = {
    storeStates.exists(_.lockedWriteTransactions.contains(rev.lastUpdateTxUUID))
  }

  private def resolve[T](storeStates: List[KeyValueObjectStoreState],
                         segments: List[Segment[T]]): Option[Restorable[T]] = if (segments.isEmpty) None else {

    val highestRevision = segments.foldLeft((HLCTimestamp.Zero, ObjectRevision.Null)) { (h,s) =>
      if (s.timestamp > h._1)
        (s.timestamp, s.revision)
      else h
    }._2

    val matches = segments.filter(_.revision == highestRevision)
    val matching = matches.size
    val mismatching = storeStates.size - matching

    //println(s"Highest Revision: $highestRevision, matching size ${matching}. Matching: $matches")

    if (matching < threshold) {
      // If we cannot restore even though we have a threshold number of responses, we need to determine whether
      // the item has been deleted or we need to wait for more responses. Deletion is determined by ruling out
      // the possibility of partial allocation. To do this, we check to see if any other store has a locked transaction
      // matching the revision of the item. If so, the item is most likely in the process of being allocated and we'll
      // need the allocation to complete before the item can be restored.
      //
      val potential = width - matching - mismatching

      if (matching + potential >= threshold || anyStoreHasLocked(highestRevision, storeStates))
        throw NotRestorable()
      else
        None // Item deleted
    } else {
      Some(Restorable(highestRevision, matches.head.timestamp, matches.map(s => s.storeID.poolIndex -> s.data)))
    }
  }
}

