package com.ibm.aspen.core.read

object ReadError extends Enumeration {
  
  /** UUID for the stored object does not match the UUID in the ObjectPointer */
  val ObjectMismatch = Value("ObjectMismatch")
  
  /** LocalPointer for this store is no longer valid for use */
  val InvalidLocalPointer = Value("InvalidLocalPointer")
  
  /** Failed checksum of object content */
  val CorruptedObject = Value("CorruptedObject")
}