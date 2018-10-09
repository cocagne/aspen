package com.ibm.aspen.core.transaction

object UpdateError extends Enumeration {
  val MissingUpdateData         = Value("MissingUpdateData")
  val ObjectMismatch            = Value("ObjectMismatch")
  val InvalidLocalPointer       = Value("InvalidLocalPointer")
  val RevisionMismatch          = Value("RevisionMismatch")
  val RefcountMismatch          = Value("RefcountMismatch")
  val Collision                 = Value("Collision")
  val CorruptedObject           = Value("CorruptedObject")
  val InsufficientFreeSpace     = Value("InsufficientFreeSpace")
  val InvalidObjectType         = Value("InvalidObjectType")
  val KeyValueRequirementError  = Value("KeyValueRequirementError")
  val TransactionTimestampError = Value("TransactionTimestampError")
}