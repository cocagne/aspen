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
  
  /** Returned during DataStore.lockOrCollide if the revision of the object changed between the initial state
   *  query and the time at which the lock operation was preformed
   * 
   */
  val RevisionMismatch = Value("RevisionMismatch")
  
  /** Returned during DataStore.lockOrCollide if the refcount of the object changed between the initial state
   *  query and the time at which the lock operation was preformed
   * 
   */
  val RefcountMismatch = Value("RefcountMismatch")
  
}