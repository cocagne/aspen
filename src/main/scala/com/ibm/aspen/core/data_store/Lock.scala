package com.ibm.aspen.core.data_store

import com.ibm.aspen.core.transaction.TransactionDescription

sealed abstract class Lock {
  val txd: TransactionDescription
}

case class RevisionWriteLock(txd: TransactionDescription) extends Lock
case class RevisionReadLock(txd: TransactionDescription) extends Lock
case class RefcountReadLock(txd: TransactionDescription) extends Lock
case class RefcountWriteLock(txd: TransactionDescription) extends Lock