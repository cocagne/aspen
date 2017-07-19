package com.ibm.aspen.core.transaction

import com.ibm.aspen.core.transaction.paxos.PersistentState
import com.ibm.aspen.core.data_store.DataStore
import java.nio.ByteBuffer

case class TransactionRecoveryState(
    store: DataStore,
    txd: TransactionDescription,
    localUpdates: Option[Array[ByteBuffer]],
    disposition: TransactionDisposition.Value,
    status: TransactionStatus.Value,
    paxosAcceptorState: PersistentState)