package com.ibm.aspen.core.transaction

import java.util.UUID

import com.ibm.aspen.core.data_store.DataStoreID
import com.ibm.aspen.core.transaction.paxos.ProposalID

sealed abstract class Message {
  val to: DataStoreID
  val from: DataStoreID
}

final case class TxPrepare(
    to: DataStoreID,
    from: DataStoreID,
    txd: TransactionDescription,
    proposalId: ProposalID) extends Message
    
final case class TxPrepareResponse(
    to: DataStoreID,
    from: DataStoreID,
    transactionUUID: UUID,
    response: Either[TxPrepareResponse.Nack, TxPrepareResponse.Promise],
    proposalId: ProposalID,
    disposition: TransactionDisposition.Value,
    errors: List[UpdateErrorResponse]) extends Message
    
object TxPrepareResponse {
  case class Nack(promisedId: ProposalID)
  case class Promise(lastAccepted: Option[(ProposalID,Boolean)])
}

final case class TxAccept(
    to: DataStoreID,
    from: DataStoreID,
    transactionUUID: UUID,
    proposalId: ProposalID,
    value: Boolean) extends Message
  
final case class TxAcceptResponse(
    to: DataStoreID,
    from: DataStoreID,
    transactionUUID: UUID,
    proposalId: ProposalID,
    response: Either[TxAcceptResponse.Nack, TxAcceptResponse.Accepted]) extends Message
    
object TxAcceptResponse {
  case class Nack(promisedId: ProposalID)
  case class Accepted(value: Boolean)
}

final case class TxResolved(
    to: DataStoreID,
    from: DataStoreID,
    transactionUUID: UUID,
    committed: Boolean) extends Message
    
final case class TxCommitted(
    to: DataStoreID,
    from: DataStoreID,
    transactionUUID: UUID,
    // List of object UUIDs that could not be committed due to transaction requirement errors
    objectCommitErrors: List[UUID]) extends Message
    
final case class TxFinalized(
    to: DataStoreID,
    from: DataStoreID,
    transactionUUID: UUID,
    committed: Boolean) extends Message
    
final case class TxHeartbeat(
    to: DataStoreID,
    from: DataStoreID,
    transactionUUID: UUID) extends Message

final case class TxStatusRequest(
    to: DataStoreID,
    from: DataStoreID,
    transactionUUID: UUID,
    requestUUID: UUID) extends Message

final case class TxStatusResponse(
    to: DataStoreID,
    from: DataStoreID,
    transactionUUID: UUID,
    requestUUID: UUID,
    status: Option[TxStatusResponse.TxStatus]) extends Message

object TxStatusResponse {
  case class TxStatus(status: TransactionStatus.Value, finalized: Boolean)
}
