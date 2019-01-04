package com.ibm.aspen.base.impl

import java.nio.ByteBuffer
import java.util.UUID

import com.ibm.aspen.base._
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

    private val allStores = txd.allDataStores
    private val numStores = allStores.size

    private[this] var executing = false
    private[this] var commitErrors: Map[DataStoreID, List[UUID]] = Map()

    addHandler(this)

    def updateCommitErrors(commitErrors: Map[DataStoreID, List[UUID]]): Unit = synchronized {
      if (!executing) {
        this.commitErrors = commitErrors
        if (commitErrors.size == numStores) {
          if (commitErrors.forall(t => t._2.isEmpty)) {
            executing = true
            removeHandler(this)
            promise.success(()) // All peers committed without error, no missed updates to log
          } else {
            markMissedPeers()
          }
        }
      }
    }

    def checkTimeout(now: Long): Unit = synchronized {
      if (!executing && now - startTime > missedCommitDelayInMs)
        markMissedPeers()
    }
    
    def markMissedPeers(): Unit = {

      executing = true
      removeHandler(this)

      // Invert commit errors map
      val reportedErrors = commitErrors.foldLeft(Map[UUID, List[Byte]]()) { (m, t) =>
        val (storeId, errorUUIDs) = t
        errorUUIDs.foldLeft(m) { (subm, objectUUID) =>
          val lst = subm.get(objectUUID) match {
            case None => storeId.poolIndex :: Nil

            case Some(l) => storeId.poolIndex :: l
          }
          subm + (objectUUID -> lst)
        }
      }

      case class MissedUpdate(pointer: ObjectPointer, stores: List[Byte])

      def markObject(mu: MissedUpdate): Future[Unit] = {

        def onFail(err: Throwable): Future[Unit] = { err match {
          case err: UnknownStoragePool =>
            // StoragePool must have been deleted
            throw StopRetrying(err)

          case _ => Future.unit
        }}

        system.retryStrategy.retryUntilSuccessful(onFail _) {
          for {
            pool <- system.getStoragePool(mu.pointer.poolUUID)
            muh = pool.createMissedUpdateHandler(txd.transactionUUID, mu.pointer, mu.stores)
            _ <- muh.execute()
          } yield ()
        }
      }
      
      val misses = txd.allReferencedObjectsSet.foldLeft(List[MissedUpdate]()) { (lmiss, ptr) =>
        val missedIndicies = ptr.hostingStores.foldLeft(reportedErrors.getOrElse(ptr.uuid, Nil)) { (l, storeId) =>
          if (commitErrors.contains(storeId))
            l
          else
            storeId.poolIndex :: l
        }
        
        if (missedIndicies.isEmpty)
          lmiss
        else 
          MissedUpdate(ptr, missedIndicies) :: lmiss
      }
      
      promise.completeWith(Future.sequence(misses.map(mu => markObject(mu))).map(_=>()))
    }
  }
}


