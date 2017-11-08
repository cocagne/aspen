package com.ibm.aspen.core.data_store

import com.ibm.aspen.core.objects.StorePointer

sealed abstract class ObjectError 

sealed abstract class ObjectReadError extends ObjectError 
sealed abstract class ObjectTransactionError extends ObjectError
  
/** Supplied LocalPointer is not understood by this store or points to an invalid location */
class InvalidLocalPointer extends ObjectReadError
  
/** The local pointer is valid but the UUID of the stored object (if any) does not match that of the read request.
 *  
 *  This can occur if the original object is deleted and its storage location has been reassigned to a
 *  new object.
 */
class ObjectMismatch extends ObjectReadError

/** Checksum stored with the object does not match the read content. 
 *  
 *  This should be due to media errors 
 */
class CorruptedObject extends ObjectReadError

/** Returned during DataStore.lockOrCollide if the revision of the object changed between the initial state
 *  query and the time at which the lock operation was preformed
 * 
 */
class RevisionMismatch extends ObjectTransactionError

/** Returned during DataStore.lockOrCollide if the refcount of the object changed between the initial state
 *  query and the time at which the lock operation was preformed
 * 
 */
class RefcountMismatch extends ObjectTransactionError
  
