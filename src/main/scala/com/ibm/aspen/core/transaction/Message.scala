package com.ibm.aspen.core.transaction

import com.ibm.aspen.core.data_store.DataStoreID
import com.ibm.aspen.core.transaction.paxos.ProposalID
import java.util.UUID

sealed abstract class Message

final case class TxPrepare(
    from: DataStoreID,
    txd: TransactionDescription,
    proposalId: ProposalID) extends Message
    
final case class TxPrepareResponse(
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
    from: DataStoreID,
    transactionUUID: UUID,
    proposalId: ProposalID,
    value: Boolean) extends Message
  
final case class TxAcceptResponse(
    from: DataStoreID,
    transactionUUID: UUID,
    proposalId: ProposalID,
    response: Either[TxAcceptResponse.Nack, TxAcceptResponse.Accepted]) extends Message
    
object TxAcceptResponse {
  case class Nack(promisedId: ProposalID)
  case class Accepted(value: Boolean)
}
    
final case class TxFinalized(
    from: DataStoreID,
    transactionUUID: UUID,
    committed: Boolean) extends Message
