package com.ibm.aspen.base.impl

import java.nio.ByteBuffer
import java.util.UUID

import com.ibm.aspen.base.{AspenSystem, FinalizationAction, FinalizationActionHandler, UpdateableFinalizationAction}
import com.ibm.aspen.core.data_store.DataStoreID
import com.ibm.aspen.core.objects.ObjectPointer
import com.ibm.aspen.core.transaction.{SerializedFinalizationAction, TransactionDescription}

import scala.concurrent.duration._
import scala.concurrent.{ExecutionContext, Future, Promise}

object MissedUpdateFinalizationAction extends FinalizationActionHandler {
  
  val typeUUID: UUID = UUID.fromString("368e7176-8fed-4c61-bb8c-d1554722c5d1")
  
  private var pendingHandlers = Map[UUID, MissedUpdateHandler]()
  
  def addHandler(h: MissedUpdateHandler): Unit = synchronized { pendingHandlers += (h.txd.transactionUUID -> h) }
  
  def removeHandler(h: MissedUpdateHandler): Unit = synchronized { pendingHandlers -= h.txd.transactionUUID }
  
  private val pollingTask = BackgroundTask.schedulePeriodic(Duration(250, MILLISECONDS)) {
    val pendingSnapshot = synchronized { pendingHandlers }
    val now = System.nanoTime() / 1000000
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

    val parentTransactionUUID: UUID = txd.transactionUUID

    private val promise = Promise[Unit]()
    
    val complete: Future[Unit] = promise.future
    
    private val startTime = System.nanoTime() / 1000000
    
    private val allPeers = txd.allDataStores
    private val numPeers = allPeers.size
    
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
      
      // TODO: Handle deleted pool
      def markObject(mu: MissedUpdate): Future[Unit] = system.retryStrategy.retryUntilSuccessful {
        for {
          pool <- system.getStoragePool(mu.pointer.poolUUID)
          muh = pool.createMissedUpdateHandler(txd.transactionUUID, mu.pointer, mu.stores)
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


