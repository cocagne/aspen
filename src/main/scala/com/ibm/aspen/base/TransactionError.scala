package com.ibm.aspen.base

import com.ibm.aspen.core.transaction.TransactionDescription

sealed abstract class TransactionError {
  def txd: TransactionDescription
}

final case class TransactionAborted(txd: TransactionDescription) extends TransactionError

final case class TransactionTimedOut(txd: TransactionDescription) extends TransactionError