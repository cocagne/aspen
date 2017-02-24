package com.ibm.aspen.core.transaction

object TransactionDisposition extends Enumeration {
  val Undetermined = Value("Undetermined")
  val VoteCommit   = Value("VoteCommit")
  val VoteAbort    = Value("VoteAbort")
}