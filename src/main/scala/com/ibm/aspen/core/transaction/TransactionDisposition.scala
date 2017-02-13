package com.ibm.aspen.core.transaction

object TransactionDisposition extends Enumeration {
  val Undetermined = Value("Undetermined")
  val Collision    = Value("Collision")
  val VoteCommi    = Value("VoteCommit")
  val VoteAbort    = Value("VoteAbort")
}