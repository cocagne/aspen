package com.ibm.aspen.core.transaction

import com.ibm.aspen.core.transaction.paxos.PersistentState
import com.ibm.aspen.core.data_store.DataStore

case class TransactionRecoveryState(
    store: DataStore,
    txd: TransactionDescription,
    localUpdates: LocalUpdateContent,
    disposition: TransactionDisposition.Value,
    status: TransactionStatus.Value,
    paxosAcceptorState: PersistentState)