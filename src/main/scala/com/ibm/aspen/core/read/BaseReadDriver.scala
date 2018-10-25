package com.ibm.aspen.core.read

import java.util.UUID

import com.ibm.aspen.base.impl.BackgroundTask
import com.ibm.aspen.core.HLCTimestamp
import com.ibm.aspen.core.data_store.{DataStoreID, ObjectReadError}
import com.ibm.aspen.core.network.ClientSideReadMessenger
import com.ibm.aspen.core.objects._
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

  // Detect invalid combinations
  val objectReader: ObjectReader = (objectPointer, readType) match {
    case (p: KeyValueObjectPointer, _:MetadataOnly) => new KeyValueObjectReader(true, p, sendReadRequest)
    case (p: KeyValueObjectPointer, _) => new KeyValueObjectReader(false, p, sendReadRequest)

    case (p: DataObjectPointer,     _:MetadataOnly) => new DataObjectReader(true, p, sendReadRequest)
    case (p: DataObjectPointer,     _:FullObject) => new DataObjectReader(false, p, sendReadRequest)

    case _ => throw new AssertionError("Invalid read combination")
  }

  protected var retryCount = 0
  
  protected val promise: Promise[Either[ReadError, ObjectState]] = Promise()
  
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



  def receiveReadResponse(response:ReadResponse): Boolean = synchronized {

    val hasLocksForKnownCommittedTransactions = response.result match {
      case Left(_) => false
      case Right(cs) => ! cs.lockedWriteTransactions.forall { txuuid =>
        getTransactionResult(txuuid) match {
          case None => true
          case Some(result) => !result
        }
      }
    }

    if (hasLocksForKnownCommittedTransactions) {
      // We know for certain that the response from this store has out-of-date information. Transactions _should_
      // resolve quickly so we'll immediately reread from the store
      sendReadRequest(response.fromStore)
    }
    else {
      objectReader.receiveReadResponse(response).foreach { e =>
        if (!promise.isCompleted) {
          val result = e match {
            case Left(ObjectReadError.InvalidLocalPointer) => Left(new InvalidObject(objectPointer))
            case Left(_) => Left(new CorruptedObject(objectPointer))
            case Right(os) =>
              // Ensure any commit transactions will use timestamps after all read objects last update time
              os match {
                case dos: DataObjectState => HLCTimestamp.update(dos.timestamp)
                case kvos: KeyValueObjectState => HLCTimestamp.update(kvos.lastUpdateTimestamp)
                case mos: MetadataObjectState => HLCTimestamp.update(mos.timestamp)
              }

              Right(os)
          }
          promise.success(result)
        }
      }
    }
    objectReader.receivedResponsesFromAllStores
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