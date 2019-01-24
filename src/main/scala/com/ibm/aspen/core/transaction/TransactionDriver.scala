package com.ibm.aspen.core.transaction

import java.util.UUID

import com.ibm.aspen.base.impl.{BackgroundTask, TransactionStatusCache}
import com.ibm.aspen.core.data_store.DataStoreID
import com.ibm.aspen.core.ida.IDA
import com.ibm.aspen.core.network.StoreSideTransactionMessenger
import com.ibm.aspen.core.objects.ObjectPointer
import com.ibm.aspen.core.transaction.paxos.{Learner, Proposer}
import org.apache.logging.log4j.scala.Logging

import scala.concurrent.{ExecutionContext, Future, Promise}
import scala.concurrent.duration.{Duration, SECONDS}

abstract class TransactionDriver(
    val storeId: DataStoreID,
    val messenger: StoreSideTransactionMessenger, 
    val txd: TransactionDescription, 
    private val finalizerFactory: TransactionFinalizer.Factory)(implicit ec: ExecutionContext) extends Logging {
  
  import TransactionDriver._
  
  def ida: IDA = txd.primaryObject.ida
  
  protected val proposer: Proposer = new Proposer(storeId.poolIndex, ida.width, ida.writeThreshold)
  protected val learner: Learner = new Learner(ida.width, ida.writeThreshold)
  protected val validAcceptorSet: Set[Byte] = txd.primaryObject.storePointers.iterator.map(sp => sp.poolIndex).toSet
  protected val allObjects: Set[ObjectPointer] = txd.allReferencedObjectsSet
  protected val primaryObjectDataStores: Set[DataStoreID] = txd.primaryObjectDataStores
  protected val allDataStores: List[DataStoreID] = txd.allDataStores.toList

  protected var resolved: Boolean = false
  protected var finalized: Boolean = false
  protected var peerDispositions: Map[DataStoreID, TransactionDisposition.Value] = Map()
  protected var acceptedPeers: Set[DataStoreID] = Set[DataStoreID]()
  protected var finalizer: Option[TransactionFinalizer] = None
  protected var knownResolved: Set[DataStoreID] = Set()
  protected var resolvedValue: Boolean = false

  // Stores UUIDs of objects that could not be updated on stores as part of a successful commit
  protected var commitErrors: Map[DataStoreID, List[UUID]] = Map()

  private val completionPromise: Promise[TransactionDescription] = Promise()

  def complete: Future[TransactionDescription] = completionPromise.future

  {
    val kind: String = if (txd.designatedLeaderUID == storeId.poolIndex)
      "Designated Leader"
    else
      "Transaction Recovery"
    logger.info(s"Driving transaction to completion ($kind): ${txd.shortString}")
  }

  complete.foreach(_ => logger.info(s"Transaction driven to completion: ${txd.transactionUUID}"))

  def printState(print: String => Unit = println): Unit = synchronized {
    val sb = new StringBuilder

    sb.append("\n")
    sb.append(s"***** Transaction Status: ${txd.transactionUUID}")
    sb.append(s"  Store: $storeId\n")
    sb.append(s"  Objects: ${txd.requirements.map(_.objectPointer)}\n")
    sb.append(s"  Resolved: $resolved. Finalized: $finalized. Result: ${learner.finalValue}\n")
    sb.append(s"  Peer Dispositions: $peerDispositions\n")
    sb.append(s"  Accepted Peers: $acceptedPeers\n")
    sb.append(s"  Finalizer: ${finalizer.map(o => o.debugStatus)}\n")
    sb.append(s"  ${txd.shortString}")

    print(sb.toString)
  }

  protected def isValidAcceptor(ds: DataStoreID): Boolean = {
    ds.poolUUID == txd.primaryObject.poolUUID && validAcceptorSet.contains(ds.poolIndex)
  }

  def shutdown(): Unit = {}

  def heartbeat(): Unit = {
    messenger.send(allDataStores.map(toStoreId => TxHeartbeat(toStoreId, storeId, txd.transactionUUID)))
  }

  def receiveTxPrepare(msg: TxPrepare): Unit = synchronized {
      proposer.updateHighestProposalId(msg.proposalId)
  }

  def receiveTxPrepareResponse(msg: TxPrepareResponse,
                               transactionCache: TransactionStatusCache): Unit = synchronized {

    if (msg.proposalId != proposer.currentProposalId)
      return

    // If the response says there is a transaction collision with a transaction we know has successfully completed,
    // there's a race condition. Drop the response and re-send the prepare. Eventually it will either be successfully
    // processed or a different error will occur
    val collisionsWithCompletedTransactions = msg.errors.nonEmpty && msg.errors.forall { e =>
      e.conflictingTransaction match {
        case None => false
        case Some((txuuid, _)) =>transactionCache.getTransactionFinalizedResult(txuuid) match {
          case None => false
          case Some(b) => b
        }
      }
    }

    if (collisionsWithCompletedTransactions) {
      // Race condition. Drop this message and re-send the prepare
      sendPrepareMessage(msg.from)
      return
    }

    msg.response match {
      case Left(nack) =>
        onNack(nack.promisedId)

      case Right(promise) =>

        peerDispositions += (msg.from -> msg.disposition)

        if (isValidAcceptor(msg.from))
          proposer.receivePromise(paxos.Promise(msg.from.poolIndex, msg.proposalId, promise.lastAccepted))

        if (proposer.prepareQuorumReached && !proposer.haveLocalProposal) {

          var canCommitTransaction = true

          // Before we can make a decision, we must ensure that we have a write-threshold number of replies for
          // each of the objects referenced by the transaction
          for (ptr <- allObjects) {
            var nReplies = 0
            var nAbortVotes = 0
            var nCommitVotes = 0
            var canCommitObject = true

            ptr.storePointers.foreach(sp => peerDispositions.get(DataStoreID(ptr.poolUUID, sp.poolIndex)).foreach(disposition => {
              nReplies += 1
              disposition match {
                case TransactionDisposition.VoteCommit => nCommitVotes += 1
                case _ =>
                  // TODO: We can be quite a bit smarter about choosing when we must abort
                  nAbortVotes += 1
              }
            }))

            if (nReplies < ptr.ida.writeThreshold)
              return

            if (nReplies != ptr.ida.width && nCommitVotes < ptr.ida.writeThreshold && ptr.ida.width - nAbortVotes >= ptr.ida.writeThreshold)
              return

            if (ptr.ida.width - nAbortVotes < ptr.ida.writeThreshold)
              canCommitObject = false

            // Once canCommitTransaction flips to false, it stays there
            canCommitTransaction = canCommitTransaction && canCommitObject
          }

          // If we get here, we've made our decision
          proposer.setLocalProposal(canCommitTransaction)

          sendAcceptMessages()
        }
    }
  }

  def receiveTxAcceptResponse(msg: TxAcceptResponse): Unit = synchronized {

    if (msg.proposalId != proposer.currentProposalId)
      return

    // We shouldn't ever receive an AcceptResponse from a non-acceptor but just to be safe...
    if (!isValidAcceptor(msg.from))
      return

    msg.response match {
      case Left(nack) =>
        onNack(nack.promisedId)

      case Right(accepted) =>
        acceptedPeers += msg.from

        learner.receiveAccepted(paxos.Accepted(msg.from.poolIndex, msg.proposalId, accepted.value))

        learner.finalValue.foreach(onResolution(_, sendResolutionMessage = true))
    }
  }

  def receiveTxResolved(msg: TxResolved): Unit = synchronized {
    knownResolved += msg.from
    onResolution(msg.committed, sendResolutionMessage=false)
  }

  def receiveTxCommitted(msg: TxCommitted): Unit = synchronized {
    knownResolved += msg.from
    commitErrors += (msg.from -> msg.objectCommitErrors)
    finalizer.foreach(_.updateCommitErrors(commitErrors))
  }

  def receiveTxFinalized(msg: TxFinalized): Unit = synchronized {
    knownResolved += msg.from
    finalizer.foreach( _.cancel() )
    onFinalized(msg.committed)
  }

  def mayBeDiscarded: Boolean = synchronized { finalized }

  protected def onFinalized(committed: Boolean): Unit = synchronized {
    if (!finalized) {
      finalized = true

      shutdown() // release retry resources

      txd.originatingClient.foreach(client => {
        logger.trace(s"Sending TxFinalized(${txd.transactionUUID}) to originating client $client)")
        messenger.send(client, TxFinalized(NullDataStoreId, storeId, txd.transactionUUID, committed))
      })

      val messages = allDataStores.map(toStoreId => TxFinalized(toStoreId, storeId, txd.transactionUUID, committed))

      logger.trace(s"Sending TxFinalized(${txd.transactionUUID}) to ${messages.head.to.poolUUID}:(${messages.map(_.to.poolIndex)})")

      messenger.send(messages)

      completionPromise.success(txd)
    }
  }

  protected def nextRound(): Unit = {
    peerDispositions = Map()
    acceptedPeers = Set()
    proposer.nextRound()
  }

  protected def sendPrepareMessages(): Unit = {
    val (proposalId, alreadyPrepared, ofinal) = synchronized { (proposer.currentProposalId, peerDispositions, learner.finalValue) }

    // Send TxResolved if resolution has been achieved, otherwise Prepare messages
    val messages = ofinal match {
      case Some(result) => txd.allDataStores.map(TxResolved(_, storeId, txd.transactionUUID, result))
      case None => txd.allDataStores.filter(!alreadyPrepared.contains(_)).map(TxPrepare(_, storeId, txd, proposalId))
    }

    if (messages.nonEmpty)
      logger.trace(s"Sending ${messages.head.getClass.getSimpleName}(${txd.transactionUUID}) to ${messages.head.to.poolUUID}:(${messages.map(_.to.poolIndex).toList})")

    messenger.send(messages.toList)
  }

  protected def sendPrepareMessage(toStoreId: DataStoreID): Unit = {
    val (proposalId, ofinal) = synchronized { (proposer.currentProposalId, learner.finalValue) }

    ofinal match {
      case Some(result) =>
        logger.trace(s"Sending TxResolved(${txd.transactionUUID}) committed = $result to $toStoreId")
        messenger.send(TxResolved(toStoreId, storeId, txd.transactionUUID, result))

      case None =>
        logger.trace(s"Sending TxPrepare(${txd.transactionUUID}) to $toStoreId")
        messenger.send(TxPrepare(toStoreId, storeId, txd, proposalId))
    }
  }

  protected def sendAcceptMessages(): Unit = {
    synchronized {
      proposer.currentAcceptMessage().map(paxAccept => (paxAccept, acceptedPeers))
    } foreach { t =>
      val (paxAccept, alreadyAccepted) = t
      val messages = primaryObjectDataStores.filter(!alreadyAccepted.contains(_)).map { toStoreId =>
        TxAccept(toStoreId, storeId, txd.transactionUUID, paxAccept.proposalId, paxAccept.proposalValue)
      }.toList
      if (messages.nonEmpty)
        logger.trace(s"Sending TxAccept(${txd.transactionUUID}) to ${messages.head.to.poolUUID}:(${messages.map(_.to.poolIndex)})")
      messenger.send(messages)
    }
  }

  protected def onNack(promisedId: paxos.ProposalID): Unit = {
    proposer.updateHighestProposalId(promisedId)
    nextRound()
  }

  protected def onResolution(committed: Boolean, sendResolutionMessage: Boolean): Unit = if (!resolved) {

    resolved = true

    resolvedValue = committed

    if (committed) {

      // TODO: If sendResolutionMessage is false, another driver sent us a resolution message and will have
      //       started executing the finalizers. Ideally, we'd watch the heartbeats of the other driver and
      //       only start the finalizers ourselves if they time out to prevent contention. Could be tricky to
      //       get this right though so we'll go with the simple approach for now

      logger.trace(s"Running finalization actions for transaction ${txd.transactionUUID}")
      val f = finalizerFactory.create(txd, messenger)

      // Update with initial commitErrors. This avoids problems when all TxCommit messages arrive
      // before we notice that resolution has been achieved
      f.updateCommitErrors(commitErrors)

      f.complete foreach { _ =>
        logger.trace(s"Finalization actions completed for transaction ${txd.transactionUUID}")
        onFinalized(committed)
      }

      finalizer = Some(f)
    } else
      onFinalized(false)

    if (sendResolutionMessage) {
      val messages = (allDataStores.iterator ++ txd.notifyOnResolution.iterator).map { toStoreId =>
        TxResolved(toStoreId, storeId, txd.transactionUUID, committed)
      }.toList

      logger.trace(s"Sending TxResolved(${txd.transactionUUID}) committed = $committed to ${messages.head.to.poolUUID}:(${messages.map(_.to.poolIndex)})")

      messenger.send(messages)

      txd.originatingClient.foreach { clientId =>
        logger.trace(s"Sending TxResolved(${txd.transactionUUID}) committed = $committed to originating client $clientId")
        messenger.send(clientId, TxResolved(NullDataStoreId, storeId, txd.transactionUUID, committed))
      }
    }
  }
}

object TransactionDriver {
  /** Used when sending Transaction Messages to Clients instead of data stores */
  val NullDataStoreId = DataStoreID(new UUID(0,0), 0)
  
  trait Factory {
    def create(
        storeId: DataStoreID,
        messenger:StoreSideTransactionMessenger, 
        txd: TransactionDescription, 
        finalizerFactory: TransactionFinalizer.Factory)(implicit ec: ExecutionContext): TransactionDriver
  }
  
  object noErrorRecoveryFactory extends Factory {
    class NoRecoveryTransactionDriver(
        storeId: DataStoreID,
        messenger: StoreSideTransactionMessenger, 
        txd: TransactionDescription, 
        finalizerFactory: TransactionFinalizer.Factory)(implicit ec: ExecutionContext) extends TransactionDriver(
      storeId, messenger, txd, finalizerFactory) {

      var hung = false

      val hangCheckTask: BackgroundTask.ScheduledTask = BackgroundTask.schedule(Duration(10, SECONDS)) {
        val test = messenger.system.map(_.getSystemAttribute("unittest.name").getOrElse("UNKNOWN TEST"))
        println(s"**** HUNG TRANSACTION: $test")
        printState()
        synchronized(hung = true)
      }

      override protected def onFinalized(committed: Boolean): Unit = {
        super.onFinalized(committed)
        synchronized {
          if (hung) {
            val test = messenger.system.map(_.getSystemAttribute("unittest.name").getOrElse("UNKNOWN TEST"))
            println(s"**** HUNG TRANSACTION EVENTUALLY COMPLETED! : $test")
          }
        }
        hangCheckTask.cancel()
      }
    }
    
    def create(
        storeId: DataStoreID,
        messenger:StoreSideTransactionMessenger, 
        txd: TransactionDescription, 
        finalizerFactory: TransactionFinalizer.Factory)(implicit ec: ExecutionContext): TransactionDriver = {
      new NoRecoveryTransactionDriver(storeId, messenger, txd, finalizerFactory)
    }
  }
}
