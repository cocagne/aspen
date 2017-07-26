package com.ibm.aspen.core.transaction

import com.ibm.aspen.core.transaction.paxos.PersistentState
import com.ibm.aspen.core.data_store.DataStore
import java.nio.ByteBuffer
import com.ibm.aspen.core.data_store.DataStoreID

case class TransactionRecoveryState(
    storeId: DataStoreID,
    txd: TransactionDescription,
    localUpdates: Option[Array[ByteBuffer]],
    disposition: TransactionDisposition.Value,
    status: TransactionStatus.Value,
    paxosAcceptorState: PersistentState)