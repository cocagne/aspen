package com.ibm.aspen.core.read

import com.ibm.aspen.core.data_store.DataStoreID
import com.ibm.aspen.core.data_store

sealed abstract class ReadError(msg: String) extends Exception(msg)

class ReadIDAError(msg:String) extends ReadError(msg)

case class ThresholdError(errors: Map[DataStoreID, ReadError.Value]) extends ReadError("ThresholdError")

case class InvalidObject() extends ReadError("InvalidObject")

case class EncodingError() extends ReadError("EncodingError")

case class UnexpectedError() extends ReadError("UnexpectedError")

object ReadError extends Enumeration {
  
  /** UUID for the stored object does not match the UUID in the ObjectPointer */
  val ObjectMismatch = Value("ObjectMismatch")
  
  /** LocalPointer for this store is no longer valid for use */
  val InvalidLocalPointer = Value("InvalidLocalPointer")
  
  /** Failed checksum of object content */
  val CorruptedObject = Value("CorruptedObject")
  
  /** IDA failed to restore data */
  val IDARestoreError = Value("IDARestoreError")
  
  /** Invalid object encoding */
  val InvalidObjectEncoding = Value("InvalidObjectEncoding")
  
  def apply(objectReadError: data_store.ObjectReadError): Value = objectReadError match {
    case _: data_store.InvalidLocalPointer => InvalidLocalPointer
    case _: data_store.ObjectMismatch => ObjectMismatch
    case _: data_store.CorruptedObject => CorruptedObject
  }
}