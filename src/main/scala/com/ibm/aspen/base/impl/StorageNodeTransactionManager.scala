package com.ibm.aspen.base.impl

import java.util.UUID

import com.github.blemale.scaffeine.Scaffeine
import com.ibm.aspen.core.crl.CrashRecoveryLog
import com.ibm.aspen.core.data_store.{DataStore, DataStoreID}
import com.ibm.aspen.core.network.{StoreSideTransactionMessageReceiver, StoreSideTransactionMessenger}
import com.ibm.aspen.core.objects.ObjectPointer
import com.ibm.aspen.core.transaction.{TransactionStatus, _}

import scala.concurrent.duration._
import scala.concurrent.{ExecutionContext, Future, Promise}

object StorageNodeTransactionManager {

  class StatusRequest(val initialRetryDelay: Duration,
                      val txmgr: StorageNodeTransactionManager,
                      val requestingStore: DataStoreID,
                      val primaryObject: ObjectPointer,
                      val transactionUUID: UUID,
                      val requireFinalized: Boolean)(implicit ec: ExecutionContext) {

    val requestUUID: UUID = UUID.randomUUID()
    val promise: Promise[Option[TransactionStatus.Value]] = Promise()
    var responses: Map[DataStoreID, Option[TransactionStatus.Value]] = Map()

    val retransmitTask: BackgroundTask.ScheduledTask = BackgroundTask.RetryWithExponentialBackoff(
      tryNow=false, initialRetryDelay, Duration(1,MINUTES)) {
      txmgr.sendStatusRequests(this)
      false
    }

    txmgr.sendStatusRequests(this)

    promise.future.onComplete(_ => retransmitTask.cancel())
  }

}

class StorageNodeTransactionManager(
    val crl: CrashRecoveryLog,
    val transactionCache: TransactionStatusCache,
    val messenger: StoreSideTransactionMessenger,
    val driverFactory: TransactionDriver.Factory,
    val finalizerFactory: TransactionFinalizer.Factory)(implicit ec: ExecutionContext)
  extends StoreSideTransactionMessageReceiver with TransactionStatusQuerier {

  import StorageNodeTransactionManager._
  
  private[this] var stores = Map[DataStoreID, StoreState]()
 
  private[this] val prepareResponseCache = Scaffeine().expireAfterWrite(10.seconds)
                                                      .maximumSize(1000)
                                                      .build[UUID, List[TxPrepareResponse]]()

  private[this] var statusRequests: Map[UUID, StatusRequest] = Map()

  def idle: Future[Unit] = synchronized {
    Future.sequence(stores.valuesIterator.map(_.idle).toList).map(_=>())
  }

  def shutdown(): Future[Unit] = synchronized {
    Future.sequence( stores.valuesIterator.map( s => s.shutdown() ) ).map(_ => ())
  }
  
  def addStore(store: DataStore): Unit = synchronized {
    val ltrs = crl.getTransactionRecoveryStateForStore(store.storeId)
    
    stores += (store.storeId -> new StoreState(store, ltrs))
  }
  
  def removeStore(storeId: DataStoreID): Unit = synchronized {
    stores -= storeId
  }
  
  private[this] def getStore(storeId: DataStoreID): Option[StoreState] = synchronized {
    stores.get(storeId)
  }
  
  protected def storesSnapshot: Map[DataStoreID, StoreState] = synchronized { stores }
  
  private def cachePrepareResponse(m: TxPrepareResponse): Unit = synchronized {
    // Do this in a synchronized block to serialize updates to the cached list
    val l = prepareResponseCache.getIfPresent(m.transactionUUID).getOrElse(Nil)
    prepareResponseCache.put(m.transactionUUID, m :: l)
  }

  private def sendStatusRequests(req: StatusRequest): Unit = req.primaryObject.storePointers.foreach { sp =>
    val to = DataStoreID(req.primaryObject.poolUUID, sp.poolIndex)
    messenger.send(TxStatusRequest(to, req.requestingStore, req.transactionUUID, req.requestUUID))
  }

  def queryTransactionStatus(requestingStore: DataStoreID,
                             initialRetryDelay: Duration,
                             primaryObject: ObjectPointer,
                             transactionUUID: UUID): Future[Option[TransactionStatus.Value]] = synchronized {
    val req = new StatusRequest(initialRetryDelay: Duration, txmgr = this, requestingStore, primaryObject,
      transactionUUID, requireFinalized = false)

    statusRequests += req.requestUUID -> req
    req.promise.future
  }

  /** The future completes when either a finalized commit is seen, an abort resolution is seen, or the number of
    * "unknown transaction" responses equals or exceeds the object's consistent restore threshold. Note that if this
    * method is called before the stores hosting the target object begin processing the transaction, the promise
    * could complete. Which would falsely indicate that the transaction had been completed, finalized, and forgotten.
    * Users of this method must ensure correct operation in that case.
    */
  def queryFinalizedTransactionStatus(requestingStore: DataStoreID,
                                      initialRetryDelay: Duration,
                                      primaryObject: ObjectPointer,
                                      transactionUUID: UUID): Future[Option[TransactionStatus.Value]] = synchronized {
    val req = new StatusRequest(initialRetryDelay: Duration, txmgr = this, requestingStore, primaryObject,
      transactionUUID, requireFinalized = true)

    statusRequests += req.requestUUID -> req
    req.promise.future
  }
  
  /** (unit test only) returns true if all transactions are complete */
  def allTransactionsComplete: Boolean = synchronized {
    stores.forall(t => t._2.allTransactionsComplete)
  }

  def logTransactionStatus(log: String => Unit): Unit = synchronized {
    stores.foreach(t => t._2.logTransactionStatus(log))
  }
  
  def receive(message: Message, updateContent: Option[List[LocalUpdate]]): Unit = {
    getStore(message.to).foreach(_.receive(message, updateContent))
  }
  
  def getTransactionStatus(storeId: DataStoreID, txUUID: UUID): Option[TransactionStatus.Value] = {
    getStore(storeId) flatMap { store =>
      store.getTransaction(txUUID).map { t =>
        t.currentTransactionStatus()
      }
    }
  }
  
  protected class StoreState(val store: DataStore, txRecoveryState: List[TransactionRecoveryState]) {
    private[this] var opIdle: Option[Promise[Unit]] = None
    private[this] var transactionDrivers: Map[UUID, TransactionDriver] = Map()
    private[this] var transactions: Map[UUID, Transaction] = txRecoveryState.map { trs => 
      trs.txd.transactionUUID -> Transaction(crl, messenger, onTransactionDiscarded, store, trs.txd, trs.localUpdates)
    }.toMap

    def idle: Future[Unit] = synchronized {
      opIdle match {
        case None => Future.unit
        case Some(p) => p.future
      }
    }
    
    def shutdown(): Future[Unit] = synchronized {
      transactionDrivers.valuesIterator.foreach( d => d.shutdown() )
      store.close()
    }
    
    def allTransactionsComplete: Boolean = synchronized { store.allTransactionsComplete }

    def logTransactionStatus(log: String => Unit): Unit = synchronized {
      store.logTransactionStatus(log)
      transactionDrivers.values.foreach(_.printState(log))
    }
    
    def getTransaction(txUUID: UUID): Option[Transaction] = synchronized { transactions.get(txUUID) }
    
    def getTransactionDriver(txUUID: UUID): Option[TransactionDriver] = synchronized { transactionDrivers.get(txUUID) }
    
    def getTransactions: (Map[UUID, Transaction], Map[UUID, TransactionDriver]) = synchronized {
      (transactions, transactionDrivers) 
    }
    
    def onTransactionDiscarded(t: Transaction): Unit = synchronized { transactions -= t.txd.transactionUUID }
  
    def onTransactionDriverComplete(txuuid: UUID): Unit = synchronized {
      transactionDrivers -= txuuid

      opIdle.foreach { p =>
        opIdle = None
        p.success(())
      }
    }
    
    def driveTransaction(txd: TransactionDescription): TransactionDriver = synchronized {
      val driver = driverFactory.create(store.storeId, messenger, txd, finalizerFactory)

      transactionDrivers += (txd.transactionUUID -> driver)

      opIdle match {
        case Some(_) =>
        case None => opIdle = Some(Promise[Unit]())
      }

      driver.complete.foreach(txd => onTransactionDriverComplete(txd.transactionUUID))

      driver
    }
    
    def receive(message: Message, updateContent: Option[List[LocalUpdate]]): Unit = {
      //println(s"Store RCV: to ${message.to} from ${message.from} class ${message.getClass}")
      message match {
        case m: TxPrepare =>
          transactionCache.getTransactionFinalizedResult(m.txd.transactionUUID) match {

            case Some(result) =>
              // If we know the transaction is already complete, most likely the driver is attempting to recover from
              // lost messages. No need to re-run the transaction when we already know the end result.
              messenger.send(TxFinalized(m.from, store.storeId, m.txd.transactionUUID, result))

            case None =>

              val (otx, odriver) = transactionCache.getTransactionResolution(m.txd.transactionUUID) match {
                case Some(result) =>
                  // If we know the transaction is already complete, most likely the driver is attempting to recover from
                  // lost messages. No need to re-run the transaction when we already know the end result.
                  messenger.send(TxResolved(m.from, store.storeId, m.txd.transactionUUID, result))

                  synchronized {
                    (None, transactionDrivers.get(m.txd.transactionUUID))
                  }

                case None =>
                  synchronized {
                    val tx = transactions.getOrElse(m.txd.transactionUUID, {

                      val t = Transaction(crl, messenger, onTransactionDiscarded, store, m.txd, updateContent)

                      transactions += (m.txd.transactionUUID -> t)

                      if (m.txd.designatedLeaderUID == store.storeId.poolIndex) {
                        val driver = driveTransaction(m.txd)

                        // If any TxPrepareResponse messages were received before we noticed that we're the transaction driver, process them now
                        prepareResponseCache.getIfPresent(m.txd.transactionUUID).foreach { lst =>
                          lst.foreach( p => driver.receiveTxPrepareResponse(p, transactionCache))

                          // No need to keep the cached entries around
                          prepareResponseCache.invalidate(m.txd.transactionUUID)
                        }
                      }

                      t
                    })

                    (Some(tx), transactionDrivers.get(m.txd.transactionUUID))
                  }
              }

              otx.foreach(_.receivePrepare(m, updateContent))
              odriver.foreach(td => td.receiveTxPrepare(m))
          }

        case m: TxPrepareResponse => 
          getTransactionDriver(m.transactionUUID)  match {
            case Some(td) => td.receiveTxPrepareResponse(m, transactionCache)
            
            case None =>
              // TxPrepareResponse messages are unicast to the transaction leader. That we're receiving one probably means we're
              // the designated leader for the transaction that either recently completed or we haven't discovered yet. If it's not
              // in the result cache, We'll hold on to these for a while so that if we receive the prepare message, we can immediately
              // process the replies rather than having to rely on the driver to recover the transaction via retransmissions or 
              // another Paxos round.
              transactionCache.getTransactionResolution(m.transactionUUID) match {
                case Some(result) => messenger.send(TxResolved(m.from, store.storeId, m.transactionUUID, result))
                
                case None => cachePrepareResponse(m)
              }              
          }
        
        case m: TxAccept => 
          getTransaction(m.transactionUUID).foreach( _.receiveAccept(m) )
        
        case m: TxAcceptResponse =>
          getTransaction(m.transactionUUID).foreach( _.receiveAcceptResponse(m) )
          getTransactionDriver(m.transactionUUID).foreach( _.receiveTxAcceptResponse(m) )
          
        case m: TxResolved =>
          if (m.committed)
            transactionCache.transactionCommitted(m.transactionUUID)
          else
            transactionCache.transactionAborted(m.transactionUUID)

          getTransaction(m.transactionUUID).foreach( _.receiveResolved(m) )
          getTransactionDriver(m.transactionUUID).foreach( _.receiveTxResolved(m) )
          
        case m: TxCommitted =>
          getTransactionDriver(m.transactionUUID).foreach( _.receiveTxCommitted(m) )
          
        case m: TxFinalized =>
          if (m.committed)
            transactionCache.transactionFinalized(m.transactionUUID)
          else
            transactionCache.transactionAborted(m.transactionUUID)

          getTransaction(m.transactionUUID).foreach( _.receiveFinalized(m) )
          getTransactionDriver(m.transactionUUID).foreach( _.receiveTxFinalized(m) )
          
        case m: TxHeartbeat => getTransaction(m.transactionUUID).foreach( _.heartbeatReceived() )

        case m: TxStatusRequest =>
          val ostatus = transactionCache.getTransactionResolution(m.transactionUUID) match {
            case Some(result) =>
              val finalized = getTransaction(m.transactionUUID).isEmpty
              val status = if (result) TransactionStatus.Committed else TransactionStatus.Aborted
              Some(TxStatusResponse.TxStatus(status, finalized))

            case None => getTransaction(m.transactionUUID) match {
              case Some(_) => Some(TxStatusResponse.TxStatus(TransactionStatus.Unresolved, finalized = false))
              case None => None
            }
          }
          messenger.send(TxStatusResponse(m.from, store.storeId, m.transactionUUID, m.requestUUID, ostatus))

        case m: TxStatusResponse => synchronized {
          statusRequests.get(m.requestUUID).foreach { r =>
            r.responses += m.from -> m.status.map(_.status)

            def complete(ostatus: Option[TransactionStatus.Value]): Unit = {
              statusRequests -= r.requestUUID
              r.promise.success(ostatus)
            }

            m.status match {
              case Some(s) =>
                if (s.finalized)
                  complete(Some(s.status))

              case _=>
            }

            if (!r.promise.isCompleted) {
              case class Counts(unknown: Int, pending: Int, committed: Int, aborted: Int)

              val counts = r.responses.valuesIterator.foldLeft(Counts(0,0,0,0)) { (c, o) =>
                o match {
                  case Some(TransactionStatus.Unresolved) => c.copy(pending = c.pending + 1)
                  case Some(TransactionStatus.Committed) => c.copy(committed = c.committed + 1)
                  case Some(TransactionStatus.Aborted) => c.copy(aborted = c.aborted + 1)
                  case Some(_) => c // should be impossible
                  case None => c.copy(unknown = c.unknown + 1)
                }
              }

              if (counts.unknown >= r.primaryObject.ida.consistentRestoreThreshold)
                complete(None)
              else if (!r.requireFinalized) {
                if (counts.committed > 0)
                  complete(Some(TransactionStatus.Committed))
                else if (counts.aborted > 0)
                  complete(Some(TransactionStatus.Aborted))
                else if (counts.pending >= r.primaryObject.ida.consistentRestoreThreshold)
                  complete(Some(TransactionStatus.Unresolved))
              }
            }
          }
        }
      }
    }
  }
 
}
