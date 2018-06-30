package com.ibm.aspen.base.impl

import java.util.UUID
import com.ibm.aspen.core.objects.ObjectPointer
import com.ibm.aspen.base.Transaction
import com.ibm.aspen.base.FinalizationActionHandler
import com.ibm.aspen.base.AspenSystem
import com.ibm.aspen.base.FinalizationAction
import com.ibm.aspen.core.data_store.DataStoreID
import com.ibm.aspen.core.transaction.TransactionDescription
import scala.concurrent.ExecutionContext
import scala.concurrent.Future
import com.ibm.aspen.base.UpdateableFinalizationAction
import java.nio.ByteBuffer
import scala.concurrent.Promise
import scala.concurrent.duration._
import com.ibm.aspen.core.transaction.SerializedFinalizationAction

object MissedUpdateFinalizationAction extends FinalizationActionHandler {
  
  val typeUUID = UUID.fromString("368e7176-8fed-4c61-bb8c-d1554722c5d1")
  
  private var pendingHandlers = Map[UUID, MissedUpdateHandler]()
  
  def addHandler(h: MissedUpdateHandler): Unit = synchronized { pendingHandlers += (h.txd.transactionUUID -> h) }
  
  def removeHandler(h: MissedUpdateHandler): Unit = synchronized { pendingHandlers -= h.txd.transactionUUID }
  
  val pollingTask = BackgroundTask.schedulePeriodic(Duration(250, MILLISECONDS), callNow = false) {
    val pendingSnapshot = synchronized { pendingHandlers }
    val now = System.nanoTime() / 1000
    pendingSnapshot.values.foreach( _.checkTimeout(now) )
  }
  
  override def createAction(
      system: AspenSystem, 
      txd: TransactionDescription,
      serializedActionData: Array[Byte])(implicit ec: ExecutionContext): FinalizationAction = {

    val missedCommitDelayInMs = ByteBuffer.wrap(serializedActionData).getInt()
    
    new MissedUpdateHandler(system, txd, missedCommitDelayInMs)      
  }
  
  def createSerializedFA(missedCommitDelayInMs: Int): SerializedFinalizationAction = {
    val arr = new Array[Byte](4)
    val bb = ByteBuffer.wrap(arr)
    bb.putInt(missedCommitDelayInMs)
    SerializedFinalizationAction(typeUUID, arr)
  }
  
  class MissedUpdateHandler(
      val system: AspenSystem, 
      val txd: TransactionDescription,
      val missedCommitDelayInMs: Int)(implicit ec: ExecutionContext) extends UpdateableFinalizationAction {
    
    val promise = Promise[Unit]()
    
    val complete = promise.future
    
    val startTime = System.nanoTime() / 1000
    
    val allPeers = txd.allDataStores
    val numPeers = allPeers.size
    
    private[this] var executing = false
    private[this] var committedPeers = Set[DataStoreID]()
    
    addHandler(this)
    
    def updateCommittedPeer(peer: DataStoreID): Unit = synchronized {
      if (!executing) {
        committedPeers += peer
        if (committedPeers.size == numPeers) {
          executing = true
          removeHandler(this)
          promise.success(()) // All peers committed, no missed updates to log
        }
      }
    }
    
    def checkTimeout(now: Long): Unit = synchronized {
      if (!executing && now - startTime > missedCommitDelayInMs) {
        executing = true
        removeHandler(this)
        markMissedPeers(allPeers &~ committedPeers)
      }
    }
    
    def markMissedPeers(missedStores: Set[DataStoreID]): Unit = {
      
      case class MissedUpdate(pointer: ObjectPointer, stores: List[Byte])
      
      def markObject(mu: MissedUpdate): Future[Unit] = {
        for {
          pool <- system.getStoragePool(mu.pointer.poolUUID)
          muh = pool.createMissedUpdateHandler(mu.pointer, mu.stores)
          _ <- muh.execute()
        } yield ()
      }
      
      val misses = txd.allReferencedObjectsSet.foldLeft(List[MissedUpdate]()) { (l, ptr) =>
        val ms = ptr.hostingStores.toSet.intersect(missedStores) 
        
        if (ms.isEmpty)
          l
        else 
          MissedUpdate(ptr, ms.map(storeId => storeId.poolIndex).toList) :: l
      }
      
      promise.completeWith(Future.sequence(misses.map(markObject(_))).map(_=>()))
    }
  }
}


