package com.ibm.aspen.core.transaction

object TransactionStatus extends Enumeration {
  val Unresolved = Value("Unresolved")
  val Committed  = Value("Committed")
  val Aborted    = Value("Aborted")
}