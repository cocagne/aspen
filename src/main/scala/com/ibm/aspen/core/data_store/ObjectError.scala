package com.ibm.aspen.core.data_store

object ObjectError extends Enumeration {
  
  /** Supplied LocalPointer is not understood by this store or points to an invalid location */
  val InvalidLocalPointer = Value("InvalidLocalPointer")
  
  /** The local pointer is valid but the UUID of the stored object (if any) does not match that of the read request.
   *  
   *  This can occur if the original object is deleted and its storage location has been reassigned to a
   *  new object.
   */
  val ObjectMismatch = Value("ObjectMismatch")
  
  /** Checksum stored with the object does not match the read content. 
   *  
   *  This should be due to media errors 
   */
  val CorruptedObject = Value("CorruptedObject")
  
}