package com.ibm.aspen.core.read

import java.util.UUID

import com.ibm.aspen.base.impl.BackgroundTask
import com.ibm.aspen.core.data_store.{DataStoreID, ObjectReadError, StoreKeyValueObjectContent}
import com.ibm.aspen.core.ida.IDAError
import com.ibm.aspen.core.network.ClientSideReadMessenger
import com.ibm.aspen.core.objects._
import com.ibm.aspen.core.objects.keyvalue.KeyValueObjectCodec
import com.ibm.aspen.core.{DataBuffer, HLCTimestamp}
import org.apache.logging.log4j.scala.Logging

import scala.concurrent.duration._
import scala.concurrent.{ExecutionContext, Future, Promise}

class BaseReadDriver(
    val getTransactionResult: UUID => Option[Boolean],
    val clientMessenger: ClientSideReadMessenger,
    val objectPointer: ObjectPointer,
    val readType: ReadType,
    val retrieveLockedTransaction: Boolean, 
    val readUUID:UUID,
    val opportunisticRebuildDelay: Duration = BaseReadDriver.DefaultOpportunisticRebuildDelay,
    val disableOpportunisticRebuild: Boolean = false
    )(implicit ec: ExecutionContext) extends ReadDriver with Logging {
  
  import BaseReadDriver._

  /*

  How to know which stores to reread from
     - Readers could maintain a rereadCandidates: Set[DataStoreID]
        on reception of new message, clear the store from the set
        While trying to restore, if we see that the store is behind on any slice/kv-pair/min/max/whatever, put in set

   */

  // Detect invalid combinations
  (objectPointer, readType) match {
    case (_:DataObjectPointer,     _:SingleKey)          => assert(false)
    case (_:DataObjectPointer,     _:LargestKeyLessThan) => assert(false)
    case (_:DataObjectPointer,     _:KeyRange)           => assert(false)
    case (_:KeyValueObjectPointer, _:ByteRange)          => assert(false)
    case _ =>
  }
  
  protected val promise: Promise[Either[ReadError, ObjectState]] = Promise()
  
  protected var storeStates: Map[DataStoreID, StoreState] = Map()
  protected var errors: Map[DataStoreID, ObjectReadError.Value] = Map()

  private var completedTransactions: Set[UUID] = Set()
  
  // Tuple is restoredObject plus the set of ObjectRevision for all contents of KeyValue objects. Empty set for DataObjects
  protected var restoredObject: Option[(ObjectState, Set[ObjectRevision])] = None
  protected var retryCount = 0
  protected var pruneCount = 0
  
  protected val readStartTs: Long = System.currentTimeMillis()
  
  def readResult: Future[Either[ReadError, ObjectState]] = promise.future
  
  def begin(): Unit = sendReadRequests()
  
  def shutdown(): Unit = {}
  
  /** Sends a Read request to the specified store. */
  protected def sendReadRequest(dataStoreId: DataStoreID): Unit = {
      clientMessenger.send(Read(dataStoreId, clientMessenger.clientId, readUUID, objectPointer, readType))
  }
      
  def receivedReplyFrom(storeId: DataStoreID): Boolean = synchronized {
    //storeStates.contains(storeId) || errors.contains(storeId)
    false
  }
      
  /** Sends a Read request to all stores that have not already responded. May be called outside a synchronized block */
  protected def sendReadRequests(): Unit = {
    logger.info(s"sending read requests for object ${objectPointer.uuid}.")
    synchronized { 
      retryCount += 1
      if (retryCount > 3)
        println(s"RESENDING READ REQUEST")
    }
    objectPointer.storePointers.foreach(sp => {
      val storeId = DataStoreID(objectPointer.poolUUID, sp.poolIndex)
      if (!receivedReplyFrom(storeId))
        sendReadRequest(storeId)
    })
  }

  /*
  private def isKnownComplete(transactionUUID: UUID): Boolean = {
    completedTransactions.contains(transactionUUID) || getTransactionResult(transactionUUID).nonEmpty
  }
  */

  private def discardResponse(storeId: DataStoreID): Unit = {
    storeStates -= storeId
    sendReadRequest(storeId)
  }

  // Filters the responses and discards + reread any that have revisions known to be behind
  private def discardOldResponses(): Unit = {
    val maxTs = storeStates.foldLeft(HLCTimestamp(0))((t, ss) => if (ss._2.timestamp > t) ss._2.timestamp else t)
    storeStates.filter(t => t._2.timestamp != maxTs).foreach(t => discardResponse(t._1))
  }

  def receiveReadResponse(response:ReadResponse): Boolean = synchronized {

    def addError(err: ObjectReadError.Value): Unit = {
      //println(s"read error ${objectPointer.uuid}: $err from store ${response.fromStore}")
      errors += (response.fromStore -> err)

      if (storeStates.contains(response.fromStore))
        storeStates -= response.fromStore
    }

    def addState(fromStore: DataStoreID, ss: StoreState): Unit = {
      storeStates += (fromStore -> ss)

      if (errors.contains(fromStore))
        errors -= fromStore
    }

    response.result match {
      case Left(err) => addError(err)

      case Right(cs) =>
        try {

          // Flag objects that are known to be out-of-date due to having slices/kv-pairs that are behind. Drop those states
          // and reread

          val ss = objectPointer match {
            case _: DataObjectPointer => new DataObjectStoreState(response.fromStore, cs.revision, cs.refcount, cs.timestamp, response.readTime, cs.sizeOnStore, cs.objectData)
            case _: KeyValueObjectPointer => new KeyValueObjectStoreState(response.fromStore, cs.revision, cs.refcount, cs.timestamp, response.readTime, cs.objectData, cs.lockedWriteTransactions)
          }

          addState(response.fromStore, ss)

          if (promise.isCompleted) {
            // TODO: Check here if opportunistic rebuild message should be sent
          }

        } catch {
          // Encoding errors here will most likely be due to application-level bugs that corrupt the state of key-value objects. The stores
          // should guard against these kinds of errors but some insidious bug or another could cause this condition to be encountered.
          case e: ObjectEncodingError =>
            logger.error(s"ObjectEncodingError $objectPointer: $e ")
            addError(ObjectReadError.CorruptedObject)
        }
    }

    if (!promise.isCompleted) {
      if (errors.size > objectPointer.ida.width - objectPointer.ida.consistentRestoreThreshold) {
        // We've received a consistentRestoreThreshold number of responses and enough of them are Fatal read errors that
        // there is no chance of ever succeeding.
        val (invalidCount, corruptCount) = errors.values.foldLeft((0, 0)) { (t, e) =>
          e match {
            case ObjectReadError.ObjectMismatch => (t._1 + 1, t._2)
            case ObjectReadError.InvalidLocalPointer => (t._1 + 1, t._2)
            case ObjectReadError.CorruptedObject => (t._1, t._2 + 1)
          }
        }
        val err = if (invalidCount >= corruptCount) new InvalidObject(objectPointer) else new CorruptedObject(objectPointer)
        promise.success(Left(err))
      }
      else
        attemptRestore()
    }

    errors.size + storeStates.size == objectPointer.ida.width
  }

  /** Checks the set of received responses to see if a consistent read has been achieved. If so, it restores the
    * object and completes the read. If responses from all stores are received and the object still cannot be
    * restored, the read state is dropped and the process begins again.
    *
    */
  private def attemptRestore(): Unit = if (storeStates.size >= objectPointer.ida.consistentRestoreThreshold) {
    /*
    Can find highest object revision & throw out + reread all responses with lower versions.
    Can also discard and reread responses with locks that are known complete.

    KV forall
      if numResponses > consist threshold:
        if have > consistent threshold. True
        else if One or more locks with matching revision exist, allocation. Return false
        else true
     */

    val (singleRevision: Map[DataStoreID, StoreState], opportunisticRebuildCandidates: Map[DataStoreID, StoreState]) = {

      val revisionCounts = storeStates.values.foldLeft(Map[ObjectRevision,Int]()) { (m, ss) =>
        m + (ss.revision -> (m.getOrElse(ss.revision, 0) + 1))
      }

      if (revisionCounts.size == 1)
        (storeStates, Map())
      else {
        val sortedCounts = revisionCounts.toList.sortBy(t => -t._2)

        if (sortedCounts.head._2 == sortedCounts.tail.head._2) {
          (Map(), Map()) // No definitive winner. Wait for more responses
        }
        else {
          val pruneSet = sortedCounts.drop(1).map(t => t._1).toSet

          storeStates.partition(t => !pruneSet.contains(t._2.revision))
        }
      }
    }

    val isRestorable = singleRevision.size >= objectPointer.ida.consistentRestoreThreshold && (objectPointer match {
      case _: DataObjectPointer => true
      case _: KeyValueObjectPointer =>
        // TODO have isRestorable return opportunistic KV rebuilds
        val kvObjectStates = storeStates.valuesIterator.map(_.asInstanceOf[KeyValueObjectStoreState].kvoss).toList
        KeyValueObjectCodec.isRestorable(objectPointer.ida, kvObjectStates, storeStates.size + errors.size)
    })

    if (isRestorable) {
      // restore threshold reached. Decode object
      try {

        val objectState = objectPointer match {
          case _: DataObjectPointer     => restoreDataObject(singleRevision)
          case _: KeyValueObjectPointer => restoreKeyValueObject(singleRevision)
        }

        restoredObject = Some((objectState._1, objectState._2))

        HLCTimestamp.update(objectState._1.timestamp)

        opportunisticRebuildCandidates.foreach { t =>
          val (storeId, ss) = t
          opportunisticRebuild(storeId, ss)
        }

        promise.success(Right(objectState._1))

      } catch {
        case _: IDAError => promise.success(Left(new CorruptedIDA(objectPointer)))
        case _: ObjectEncodingError => promise.success(Left(new CorruptedContent(objectPointer)))
        case err: Throwable =>
          logger.error(s"Unexpected exception during object restoration: $err")
          com.ibm.aspen.util.printStack()
          promise.success(Left(new CorruptedObject(objectPointer)))
      }
    }
    else if (storeStates.size + errors.size == objectPointer.ida.width)
      retryRead()
    else
      considerRetryingRead()
  }

  /** Drops currently read state and reattempts the read from scratch */
  protected def retryRead(): Unit = synchronized {
    storeStates = Map()
    errors = Map()
    sendReadRequests()
  }

  /** Called every time a message is received after we've received >= the consistent read threshold but cannot
    * restore the object.
    */
  protected def considerRetryingRead(): Unit = {}

  private def getMetadata(singleRevision: Map[DataStoreID, StoreState]): (ObjectRevision, ObjectRefcount, HLCTimestamp, HLCTimestamp) = {
    // The current refcount is the one with the highest updateSerial
    val refcount = singleRevision.foldLeft(ObjectRefcount(-1,0))((ref, t) => if (t._2.refcount.updateSerial > ref.updateSerial) t._2.refcount else ref)
    val revision = singleRevision.head._2.revision
    val timestamp = singleRevision.head._2.timestamp

    val readTime = singleRevision.foldLeft(HLCTimestamp.now)((maxts, ss) => if (ss._2.readTimestamp > maxts) ss._2.readTimestamp else maxts)

    (revision, refcount, timestamp, readTime)
  }

  private def restoreDataObject(singleRevision: Map[DataStoreID, StoreState]) : (ObjectState, Set[ObjectRevision]) = {
    val (revision, refcount, timestamp, readTime) = getMetadata(singleRevision)

    val sizeOnStore = singleRevision.head._2.asInstanceOf[DataObjectStoreState].sizeOnStore

    val segments = singleRevision.filter(t => t._2.asInstanceOf[DataObjectStoreState].objectData.isDefined).foldLeft(List[(Byte, DataBuffer)]()) { (l, t) =>
      (t._1.poolIndex, t._2.asInstanceOf[DataObjectStoreState].objectData.get) :: l
    }

    try {
      // If something goes wrong with the IDA, it'll throw an IDAError exception
      val objectState = readType match {
        case _: MetadataOnly => MetadataObjectState(objectPointer, revision, refcount, timestamp, readTime)
        case _: FullObject => DataObjectState(objectPointer.asInstanceOf[DataObjectPointer], revision, refcount, timestamp, readTime, sizeOnStore, objectPointer.ida.restore(segments))
        case _: ByteRange => DataObjectState(objectPointer.asInstanceOf[DataObjectPointer], revision, refcount, timestamp, readTime, sizeOnStore, objectPointer.ida.restore(segments))
        case _ => throw new Exception("Invalid Read Type")
      }
      (objectState, Set())
    } catch {
      case t: IDAError =>
        logger.error(s"IDA ERROR Segments: $segments")
        throw t
    }
  }

  private def restoreKeyValueObject(singleRevision: Map[DataStoreID, StoreState]): (ObjectState, Set[ObjectRevision]) = {
    val (revision, refcount, timestamp, readTime) = getMetadata(singleRevision)

    val lkvoss = storeStates.map(t => t._1.poolIndex.asInstanceOf[Int] -> t._2.asInstanceOf[KeyValueObjectStoreState].kvoss).toList

    def decode() = {

      val kvos = KeyValueObjectCodec.decode(objectPointer.asInstanceOf[KeyValueObjectPointer], revision, refcount, timestamp, readTime, lkvoss)

      (kvos, kvos.allUpdates)
    }

    val objectState = readType match {
      case _: MetadataOnly       => (MetadataObjectState(objectPointer, revision, refcount, timestamp, readTime), Set[ObjectRevision]())
      case _: FullObject         => decode()
      case _: SingleKey          => decode()
      case _: LargestKeyLessThan => decode()
      case _: KeyRange           => decode()
      case _ => throw new Exception("Invalid Read Type")
    }

    (objectState._1, objectState._2)
  }

  def opportunisticRebuild(storeId: DataStoreID, ss: StoreState): Unit = readType match {
    case _: FullObject => restoredObject.foreach { t =>
      if (disableOpportunisticRebuild)
        return // skip if disabled

      val (objectState, currentUpdateSet) = t

      val repair = objectState match {
        case d: DataObjectState => d.timestamp > ss.timestamp && (d.revision != ss.revision || d.refcount != ss.refcount)

        case k: KeyValueObjectState =>
          if (k.revision != ss.revision || k.refcount != ss.refcount)
            true
          else
            ss.asInstanceOf[KeyValueObjectStoreState].kvoss.allUpdates != currentUpdateSet

        case _: MetadataObjectState => false
      }

      if (repair && objectState.lastUpdateTimestamp > ss.lastUpdateTimestamp && objectState.lastUpdateTimestamp - ss.lastUpdateTimestamp > opportunisticRebuildDelay) {
        logger.info(s"Sending Opportunistic Rebuild to store ${storeId.poolIndex} for object ${objectPointer.uuid}")

        val data = objectState.getRebuildDataForStore(storeId).get

        val oldUpdates = ss match {
          case _: DataObjectStoreState => Set[ObjectRevision]()
          case k: KeyValueObjectStoreState => k.kvoss.allUpdates
        }

        val msg = OpportunisticRebuild(storeId, clientMessenger.clientId, objectPointer,
          ss.revision, ss.refcount, oldUpdates,
          objectState.revision, objectState.refcount, objectState.timestamp, data)

        clientMessenger.send(msg)
      }
    }

    case _ =>
  }
}

object BaseReadDriver {

  val DefaultOpportunisticRebuildDelay = Duration(5, SECONDS)


  def noErrorRecoveryReadDriver(ec: ExecutionContext)(
      getTransactionResult: UUID => Option[Boolean],
      clientMessenger: ClientSideReadMessenger,
      objectPointer: ObjectPointer,
      readType: ReadType,
      retrieveLockedTransaction: Boolean,
      readUUID:UUID,
      disableOpportunisticRebuild: Boolean): ReadDriver = {
    new BaseReadDriver(getTransactionResult, clientMessenger, objectPointer, readType, retrieveLockedTransaction, readUUID)(ec) {

      var hung = false

      val hangCheckTask: BackgroundTask.ScheduledTask = BackgroundTask.schedule(Duration(10, SECONDS)) {
        val test = clientMessenger.system.map(_.getSystemAttribute("unittest.name").getOrElse("UNKNOWN TEST"))
        println(s"**** HUNG READ: $test")

        storeStates.keysIterator.toList.sortBy(_.poolIndex).foreach { sid =>
          storeStates(sid).printDebug()
        }

       // KeyValueObjectCodec.isRestorable(objectPointer.ida,
       //   storeStates.valuesIterator.map(ss => ss.asInstanceOf[KeyValueObjectStoreState].kvoss).toList)

        synchronized(hung = true)
      }

      readResult.foreach { _ =>
        hangCheckTask.cancel()
        synchronized {
          if (hung) {
            val test = clientMessenger.system.map(_.getSystemAttribute("unittest.name").getOrElse("UNKNOWN TEST"))
            println(s"**** HUNG READ EVENTUALLY COMPLETED! : $test")
          }
        }
      }(ec)
    }
  }
}