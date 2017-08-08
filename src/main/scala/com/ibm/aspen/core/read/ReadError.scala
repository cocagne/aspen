package com.ibm.aspen.core.read

import com.ibm.aspen.core.data_store.DataStoreID

sealed abstract class ReadError(msg: String) extends Exception(msg)

class IDAError(msg:String) extends ReadError(msg)

case class ThresholdError(errors: Map[DataStoreID,Option[ReadError.Value]]) extends ReadError("ThresholdError")

case class DataRetrievalFailed() extends ReadError("DataRetrievalFailed")

object ReadError extends Enumeration {
  
  /** UUID for the stored object does not match the UUID in the ObjectPointer */
  val ObjectMismatch = Value("ObjectMismatch")
  
  /** LocalPointer for this store is no longer valid for use */
  val InvalidLocalPointer = Value("InvalidLocalPointer")
  
  /** Failed checksum of object content */
  val CorruptedObject = Value("CorruptedObject")
  
  /** Unexpected Internal error. Should only be used if bugs are encountered like receiving a RevisionMismatch error when trying to simply read an object */
  val UnexpectedInternalError = Value("UnexpectedInternalError")
  
  /** IDA failed to restore data */
  val IDARestoreError = Value("IDARestoreError")
  
  /** No responses received from the Data Store */
  val NoResponse = Value("NoResponse")
}