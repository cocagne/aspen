package com.ibm.aspen.core.read

import java.util.UUID

import com.ibm.aspen.base.AspenSystem
import com.ibm.aspen.base.impl.{BackgroundTask, TransactionStatusCache}
import com.ibm.aspen.core.data_store.DataStoreID
import com.ibm.aspen.core.network.{ClientSideReadMessageReceiver, ClientSideReadMessenger}
import com.ibm.aspen.core.objects.{ObjectPointer, ObjectState}
import org.apache.logging.log4j.scala.Logging

import scala.concurrent.{ExecutionContext, Future, Promise}
import scala.concurrent.duration._

class ClientReadManager(
    val system: AspenSystem,
    val transactionCache: TransactionStatusCache,
    val clientMessenger: ClientSideReadMessenger)(implicit ec: ExecutionContext)
  extends ClientSideReadMessageReceiver with Logging {
  
  private[this] var outstandingReads = Map[UUID, ReadDriver]()
  private[this] var completionTimes = Map[UUID, (Long, ReadDriver)]()
  private[this] var completionQueries = Map[UUID, CompletionQuery]()

  private class CompletionQuery(val pointer: ObjectPointer, val transactionUUID: UUID) {
    val queryUUID: UUID = UUID.randomUUID()
    val promise: Promise[Unit] = Promise()
    var responses: Map[DataStoreID, Boolean] = Map()

    private val retransmitTask = BackgroundTask.RetryWithExponentialBackoff(tryNow = true, Duration(10, SECONDS), Duration(3, MINUTES)) {
      pointer.hostingStores.foreach { storeId =>
        clientMessenger.send(TransactionCompletionQuery(storeId, clientMessenger.clientId, queryUUID, transactionUUID))
      }
      false
    }

    completionQueries += queryUUID -> this

    def update(storeId: DataStoreID, complete: Boolean): Unit = {
      responses += storeId -> complete
      if (responses.valuesIterator.count(b => b) >= pointer.ida.consistentRestoreThreshold) {
        completionQueries -= queryUUID
        retransmitTask.cancel()
        promise.success(())
      }
    }
  }

  /** To facilitate Opportunistic Rebuild, we'll hold on to reads after they complete until we either hear from all
    * stores or pass a fixed delay after resolving the read. This way, read responses received after the consistent
    * read threshold is achieved will still make their way to the read driver and potentially result in opportunistic
    * rebuild messages.
    */
  val pruneStaleReadsTask: BackgroundTask.ScheduledTask = BackgroundTask.schedulePeriodic(Duration(1, SECONDS)) {
    val completionSnap = synchronized { completionTimes }
    val now = System.nanoTime()/1000000
    val prune = completionSnap.filter( t => (now - t._2._1) > t._2._2.opportunisticRebuildManager.slowReadReplyDuration.toMillis )
    if (prune.nonEmpty) { synchronized {
      prune.foreach { t =>
        completionTimes -= t._1
        outstandingReads -= t._1
      }
    }}
  }
  
  override def receive(m: ReadResponse): Unit = {
    synchronized { outstandingReads.get(m.readUUID) } match {
      case None => logger.warn(s"Received ReadResponse for UNKNOWN read ${m.readUUID}")

      case Some(driver) =>
        val wasCompleted = driver.readResult.isCompleted
        val allResponded = driver.receiveReadResponse(m)
        val isCompleted = driver.readResult.isCompleted
        
        if (isCompleted) {
          synchronized {
            if (allResponded) {
              completionTimes -= m.readUUID
              outstandingReads -= m.readUUID
            }
            else if (!wasCompleted)
              completionTimes += (m.readUUID -> (System.nanoTime()/1000000, driver))
          }
        }
    }
  }

  override def receive(m: TransactionCompletionResponse): Unit = synchronized {
    completionQueries.get(m.queryUUID).foreach(_.update(m.fromStore, m.isComplete))
  }
  
  
  def shutdown(): Unit = synchronized {
    outstandingReads.foreach( t => t._2.shutdown() )
  }
  
  /** Creates a ReadDriver from the passed-in factory function and returns a Future to the eventual result.
   * 
   * The purpose of using a factory here is to allow for flexibility in the strategies used to drive the reads. Reads of objects
   * that are known to exist at the same site as the client might use smaller timeouts or more more aggressive retry logic.
   * 
   * driverFactory - First boolean is for retrieve data, second is for retrieve transaction locks
   */
  def read(
      objectPointer: ObjectPointer, 
      readType: ReadType,
      retrieveTransactionLocks:Boolean=false,
      disableOpportunisticRebuild:Boolean=false,
      driverFactory: ReadDriver.Factory): Future[Either[ReadError, ObjectState]] = {
    
    val readUUID = UUID.randomUUID()

    val driver = driverFactory(system.objectCache, system.opportunisticRebuildManager, transactionCache,
      clientMessenger, objectPointer, readType, retrieveTransactionLocks, readUUID, disableOpportunisticRebuild)
                                      
    synchronized { outstandingReads += (readUUID -> driver) }
    
    driver.begin()

    driver.readResult
  }

  def getTransactionFinalized(pointer: ObjectPointer, transactionUUID: UUID): Future[Unit] = {
    val q = new CompletionQuery(pointer, transactionUUID)
    q.promise.future
  }
}