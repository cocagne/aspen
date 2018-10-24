package com.ibm.aspen.core.read

import com.ibm.aspen.core.HLCTimestamp
import com.ibm.aspen.core.data_store.{DataStoreID, ObjectReadError}
import com.ibm.aspen.core.objects._

abstract class ObjectReader[PointerType <: ObjectPointer, StoreStateType <: StoreState](
    val metadataOnly: Boolean,
    val pointer: PointerType,
    protected val reread: DataStoreID => Unit) {

  def width: Int = pointer.ida.width
  def threshold: Int = pointer.ida.consistentRestoreThreshold

  protected var responses: Map[DataStoreID, Either[ObjectReadError.Value, StoreStateType]] = Map()
  protected var endResult: Option[Either[ObjectReadError.Value, ObjectState]] = None
  protected var knownBehind: Map[DataStoreID, HLCTimestamp] = Map()

  /** Returns the map of store ids that are known to have returned responses with out-of-date results. The value
    * is the read time of the returned read response. If a reread is also out-of-date the timestamp value will be
    * updated. If the response is fully up-to-date, the store's entry will be removed from the map
    */
  def rereadCandidates: Map[DataStoreID, HLCTimestamp] = knownBehind

  def result: Option[Either[ObjectReadError.Value, ObjectState]] = endResult

  protected def createObjectState(storeId:DataStoreID, readTime: HLCTimestamp, cs: ReadResponse.CurrentState): StoreStateType

  /** Called with a list of store states with matching, highest-seen revisions. The list will contain >= threshold
    * elements
    */
  protected def restoreObject(revision:ObjectRevision, refcount: ObjectRefcount, timestamp:HLCTimestamp,
                              readTime: HLCTimestamp, storeStates: List[StoreStateType]): Unit

  def numErrors: Int = responses.valuesIterator.foldLeft(0) { (count, e) => e match {
    case Left(_) => count + 1
    case Right(_) => count
  }}

  def receiveReadResponse(response:ReadResponse): Option[Either[ObjectReadError.Value, ObjectState]] = {
    knownBehind -= response.fromStore // Start fresh for this node

    response.result match {
      case Left(err) => responses += response.fromStore -> Left(err)

      case Right(cs) =>
        try {
          responses += response.fromStore -> Right(createObjectState(response.fromStore, response.readTime, cs))
        } catch {
          case _: ObjectEncodingError =>
            responses += response.fromStore -> Left(ObjectReadError.CorruptedObject)
        }
    }

    restore()
  }

  protected def restore(): Option[Either[ObjectReadError.Value, ObjectState]] = endResult match {
    case Some(r) => Some(r)
    case None =>
      if (responses.size >= threshold) {

        if (numErrors > width - threshold) {
          val (invalidCount, corruptCount) = responses.values.foldLeft((0, 0)) { (t, e) =>
            e match {
              case Left(ObjectReadError.ObjectMismatch) => (t._1 + 1, t._2)
              case Left(ObjectReadError.InvalidLocalPointer) => (t._1 + 1, t._2)
              case Left(ObjectReadError.CorruptedObject) => (t._1, t._2 + 1)
              case Left(_) => t
              case Right(_) => t
            }
          }
          val err = if (invalidCount >= corruptCount)
            ObjectReadError.InvalidLocalPointer
          else
            ObjectReadError.CorruptedObject

          endResult = Some(Left(err))

        }
        else
          attemptRestore()
      }

      endResult
  }

  protected def attemptRestore(): Unit = {
    val mostRecent = responses.valuesIterator.foldLeft((ObjectRevision.Null, HLCTimestamp.Zero)) { (t,r) => r match {
      case Left(_) => t
      case Right(ss) => if (ss.timestamp > t._2) (ss.revision, ss.timestamp) else t
    }}

    //val storeStates = responses.values.collect{ case Right(ss) if ss.revision == mostRecent._1 => ss }.toList

    val storeStates = responses.valuesIterator.collect { case Right(ss) => ss }.filter { ss =>
      if (mostRecent._1 == ss.revision) true else {
        knownBehind += ss.storeId -> ss.readTimestamp
        false
      }
    }.toList

    if (storeStates.size >= threshold) {
      // The current refcount is the one with the highest updateSerial
      val refcount = storeStates.foldLeft(ObjectRefcount(-1,0))((ref, ss) => if (ss.refcount.updateSerial > ref.updateSerial) ss.refcount else ref)
      val revision = storeStates.head.revision
      val timestamp = storeStates.head.timestamp

      val readTime = storeStates.foldLeft(HLCTimestamp.now)((maxts, ss) => if (ss.readTimestamp > maxts) ss.readTimestamp else maxts)

      if (metadataOnly)
        endResult = Some(Right(MetadataObjectState(pointer, revision, refcount, timestamp, readTime)))
      else
        restoreObject(revision, refcount, timestamp, readTime, storeStates)
    }
    else {
      responses.values.foreach {
        case Left(_) =>
        case Right(ss) => if (ss.revision != mostRecent._1 && !ss.rereadRequired) {
          ss.rereadRequired = true
          reread(ss.storeId)
        }
      }
    }
  }
}
