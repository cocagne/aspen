package com.ibm.amoeba.server

import java.util.concurrent.{LinkedBlockingQueue, TimeUnit}

import com.ibm.amoeba.common.network._
import com.ibm.amoeba.common.store.StoreId
import com.ibm.amoeba.common.transaction.TransactionStatus
import com.ibm.amoeba.server.crl.{CrashRecoveryLogFactory, SaveCompletion, SaveCompletionHandler}
import com.ibm.amoeba.server.network.Messenger
import com.ibm.amoeba.server.store.backend.{Backend, Completion, CompletionHandler}
import com.ibm.amoeba.server.store.cache.ObjectCache
import com.ibm.amoeba.server.store.{Frontend, Store}
import com.ibm.amoeba.server.transaction.{TransactionDriver, TransactionFinalizer, TransactionStatusCache}

import scala.concurrent.{ExecutionContext, Future, Promise}

object StoreManager {
  sealed abstract class Event

  case class IOCompletion(op: Completion) extends Event
  case class CRLCompletion(op: SaveCompletion) extends Event
  case class TransactionMessage(msg: TxMessage) extends Event
  case class ClientReq(msg: ClientRequest) extends Event
  case class LoadStore(backend: Backend) extends Event
  case class Exit() extends Event
  case class RecoveryEvent() extends Event

  class IOHandler(mgr: StoreManager) extends CompletionHandler {
    override def complete(op: Completion): Unit = {
      mgr.events.add(IOCompletion(op))
    }
  }

  class CRLHandler(mgr: StoreManager) extends SaveCompletionHandler {
    override def saveComplete(op: SaveCompletion): Unit = {
      mgr.events.add(CRLCompletion(op))
    }
  }
}

class StoreManager(val objectCache: ObjectCache,
                   val net: Messenger,
                   crlFactory: CrashRecoveryLogFactory,
                   val finalizerFactory: TransactionFinalizer.Factory,
                   val txDriverFactory: TransactionDriver.Factory,
                   initialBackends: List[Backend]) {
  import StoreManager._

  private val events = new LinkedBlockingQueue[Event]()

  private val ioHandler = new IOHandler(this)
  private val crlHandler = new CRLHandler(this)

  private val txStatusCache = new TransactionStatusCache()

  private val crl = crlFactory.createCRL(crlHandler)

  protected var shutdownCalled = false
  private val shutdownPromise: Promise[Unit] = Promise()

  protected var stores: Map[StoreId, Store] = Map()

  def loadStore(backend: Backend): Unit = {
    events.add(LoadStore(backend))
  }

  def receiveTransactionMessage(msg: TxMessage): Unit = {
    events.add(TransactionMessage(msg))
  }

  def receiveClientRequest(msg: ClientRequest): Unit = {
    events.add(ClientReq(msg))
  }

  def shutdown()(implicit ec: ExecutionContext): Future[Unit] = {
    events.add(Exit())
    shutdownPromise.future
  }

  protected def addRecoveryEvent(): Unit = events.add(RecoveryEvent())

  /** Placeholder for mixin class to implement transaction and allocation recovery */
  protected def handleRecoveryEvent(): Unit = ()

  /** Handles all events in the event queue. Returns when the queue is empty */
  protected def handleEvents(): Unit = {
    var event = events.poll(0, TimeUnit.NANOSECONDS)
    while (event != null) {
      handleEvent(event)
      event = events.poll(0, TimeUnit.NANOSECONDS)
    }
  }

  /** Preforms a blocking poll on the event queue, awaitng the delivery of an event */
  protected def awaitEvent(): Unit = {
    handleEvent(events.poll(1, TimeUnit.DAYS))
  }

  private def handleEvent(event: Event): Unit = {
    event match {

      case IOCompletion(op) => stores.get(op.storeId).foreach { store =>
        store.frontend.backendOperationComplete(op)
      }

      case CRLCompletion(op) => stores.get(op.storeId).foreach { store =>
        store.frontend.crlSaveComplete(op)
      }

      case TransactionMessage(msg) => stores.get(msg.to).foreach { store =>
        store.frontend.receiveTransactionMessage(msg)
      }

      case ClientReq(msg) => stores.get(msg.toStore).foreach { store =>
        msg match {
          case a: Allocate => store.frontend.allocateObject(a)

          case r: Read =>
            r.objectPointer.getStoreLocater(store.storeId).foreach { locater =>
              store.frontend.readObjectForNetwork(r.fromClient, r.readUUID, locater)
            }

          case op: OpportunisticRebuild => store.frontend.readObjectForOpportunisticRebuild(op)

          case s: TransactionCompletionQuery =>
            val isComplete = txStatusCache.getStatus(s.transactionId) match {
              case None => false
              case Some(e) => e.status match {
                case TransactionStatus.Unresolved => false
                case _ => true
              }
            }
            val r = TransactionCompletionResponse(s.fromClient, s.toStore, s.queryUUID, isComplete)
            net.sendClientResponse(r)
        }
      }

      case RecoveryEvent() => handleRecoveryEvent()

      case LoadStore(backend) =>
        val store = new Store(backend, objectCache, net, crl, txStatusCache,finalizerFactory, txDriverFactory)
        backend.setCompletionHandler(ioHandler)
        stores += backend.storeId -> store

      case null => // nothing to do

      case _:Exit =>
        shutdownCalled = true
        shutdownPromise.success(())
    }
  }
}