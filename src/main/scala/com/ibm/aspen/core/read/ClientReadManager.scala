package com.ibm.aspen.core.read

import com.ibm.aspen.core.network.ClientSideReadMessenger

import scala.concurrent.ExecutionContext
import java.util.UUID

import com.ibm.aspen.base.AspenSystem

import scala.concurrent.Future
import com.ibm.aspen.core.data_store.DataStoreID
import com.ibm.aspen.core.objects.ObjectPointer
import com.ibm.aspen.core.network.ClientSideReadMessageReceiver
import com.ibm.aspen.core.objects.ObjectState
import com.ibm.aspen.core.transaction.TransactionDescription
import com.ibm.aspen.core.data_store.Lock

import scala.concurrent.duration._
import com.ibm.aspen.base.impl.BackgroundTask

class ClientReadManager(
    val system: AspenSystem,
    val getTransactionResult: (UUID) => Option[Boolean], 
    val clientMessenger: ClientSideReadMessenger)(implicit ec: ExecutionContext) extends ClientSideReadMessageReceiver {
  
  private[this] var outstandingReads = Map[UUID, ReadDriver]()
  private[this] var completionTimes = Map[UUID, (Long, ReadDriver)]()
  
  val pruneStaleReadsTask = BackgroundTask.schedulePeriodic(Duration(1, SECONDS), callNow=false) {
    val completionSnap = synchronized { completionTimes }
    val now = System.nanoTime()/1000000
    val prune = completionSnap.filter( t => (now - t._2._1) > t._2._2.opportunisticRebuildDelay.toMillis )
    if (!prune.isEmpty) { synchronized {
      prune.foreach { t =>
        completionTimes -= t._1
        outstandingReads -= t._1
      }
    }}
  }
  
  def receive(m: ReadResponse): Unit = { 
    synchronized { outstandingReads.get(m.readUUID) } foreach {
      driver =>
        val wasCompleted = driver.readResult.isCompleted
        val allResponded = driver.receiveReadResponse(m)
        val isCompleted = driver.readResult.isCompleted
        
        if (isCompleted) { synchronized {
          if (allResponded) {
            completionTimes -= m.readUUID
            outstandingReads -= m.readUUID
          }
          else if (!wasCompleted)
            completionTimes += (m.readUUID -> (System.nanoTime()/1000000, driver))
        }}
    }
  }  
  
  
  def shutdown(): Unit = {
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
    
    val driver = driverFactory(getTransactionResult, clientMessenger, objectPointer, readType, retrieveTransactionLocks, readUUID, disableOpportunisticRebuild)
                                      
    synchronized { outstandingReads += (readUUID -> driver) }
    
    driver.begin()

    driver.readResult
  }
}