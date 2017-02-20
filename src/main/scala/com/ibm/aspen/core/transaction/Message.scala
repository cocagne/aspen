package com.ibm.aspen.core.transaction

import com.ibm.aspen.core.data_store.DataStoreID
import com.ibm.aspen.core.transaction.paxos.ProposalID
import java.util.UUID

sealed abstract class Message

case class TxPrepare(
    from: DataStoreID,
    txd: TransactionDescription,
    proposalId: ProposalID)
    
case class TxPrepareResponse(
    from: DataStoreID,
    transactionUUID: UUID,
    response: Either[TxPrepareResponse.Nack, TxPrepareResponse.Promise],
    proposalId: ProposalID,
    disposition: TransactionDisposition.Value,
    errors: List[UpdateErrorResponse])
    
object TxPrepareResponse {
  case class Nack(promisedId: ProposalID)
  case class Promise(lastAccepted: Option[(ProposalID,Boolean)])
}

case class TxAccept(
    from: DataStoreID,
    transactionUUID: UUID,
    proposalId: ProposalID,
    value: Boolean)
  
case class TxAccepted(
    from: DataStoreID,
    transactionUUID: UUID,
    proposalId: ProposalID,
    value: Boolean)
    
case class TxFinalized(
    from: DataStoreID,
    transactionUUID: UUID,
    committed: Boolean)
