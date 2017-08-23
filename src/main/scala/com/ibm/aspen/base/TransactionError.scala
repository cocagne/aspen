package com.ibm.aspen.base

import com.ibm.aspen.core.transaction.TransactionDescription

sealed abstract class TransactionError 

abstract class TransactionCreationError extends TransactionError {
  def reason: Throwable
}

/* Used when procedure adding content to a transaction detects a logical flaw that causes the overall
 * Transaction to become invalid. 
 * 
 * For example, while attempting to insert key/value pairs into a BTree node it could be discovered that one or more
 * of the keys is outside the valid key range owned by the node. In this case, the transaction cannot be allowed to
 * succeed so the transaction will be failed and the "reason" attribute will be set to an instance of KeyOutOfRange. 
 * 
 */
final case class InvalidTransaction(reason: Throwable) extends TransactionCreationError



abstract class TransactionProcessingError extends TransactionError {
  def txd: TransactionDescription
}

final case class TransactionAborted(txd: TransactionDescription) extends TransactionProcessingError

final case class TransactionTimedOut(txd: TransactionDescription) extends TransactionProcessingError

