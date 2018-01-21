package com.ibm.aspen.core.data_store

import com.ibm.aspen.core.transaction.TransactionDescription
import com.ibm.aspen.core.objects.keyvalue.Key

case class LockState(
    objectRevisionReadLocks: List[TransactionDescription],
    objectRevisionWriteLock: Option[TransactionDescription],
    objectRefcountReadLocks: List[TransactionDescription],
    objectRefcountWriteLocks: Option[TransactionDescription],
    keyRevisionReadLocks: List[TransactionDescription],
    keyRevisionWriteLock: Map[Key, TransactionDescription])