package com.ibm.aspen.core.data_store

import com.ibm.aspen.core.objects.ObjectPointer
import com.ibm.aspen.core.objects.ObjectRevision
import com.ibm.aspen.core.objects.ObjectRefcount
import com.ibm.aspen.core.transaction.TransactionDescription
import com.ibm.aspen.core.objects.keyvalue.Key

sealed abstract class ObjectError 

sealed abstract class ObjectReadError extends ObjectError

/** Enumerated values that correspond to Exceptions types. Used as error codes for network messages.
 */
object ObjectReadError extends Enumeration {
  val InvalidLocalPointer = Value("InvalidLocalPointer")
  val ObjectMismatch = Value("ObjectMismatch")
  val CorruptedObject = Value("CorruptedObject")
  
  def apply(err: ObjectReadError): Value = err match {
    case t: InvalidLocalPointer => InvalidLocalPointer
    case t: ObjectMismatch => ObjectMismatch
    case t: CorruptedObject => CorruptedObject
  }
}
  
/** Supplied LocalPointer is not understood by this store or points to an invalid location */
case class InvalidLocalPointer() extends ObjectReadError
  
/** The local pointer is valid but the UUID of the stored object (if any) does not match that of the read request.
 *  
 *  This can occur if the original object is deleted and its storage location has been reassigned to a
 *  new object.
 */
case class ObjectMismatch() extends ObjectReadError

/** Checksum stored with the object does not match the read content. 
 *  
 *  This should be due to media errors 
 */
case class CorruptedObject() extends ObjectReadError


//--------------------------------------------------------------------

sealed abstract class ObjectTransactionError extends ObjectError {
  val objectPointer: ObjectPointer
}

/** Used to indicate an ObjectReadError occurred as part of a transaction
 * 
 */
case class TransactionReadError(objectPointer: ObjectPointer, kind: ObjectReadError) extends ObjectTransactionError

/** Returned during DataStore.lockOrCollide if the revision of the object changed between the initial state
 *  query and the time at which the lock operation was preformed
 * 
 */
case class RevisionMismatch(objectPointer: ObjectPointer, required: ObjectRevision, current: ObjectRevision) extends ObjectTransactionError

/** Returned during DataStore.lockOrCollide if the refcount of the object changed between the initial state
 *  query and the time at which the lock operation was preformed
 * 
 */
case class RefcountMismatch(objectPointer: ObjectPointer, required: ObjectRefcount, current: ObjectRefcount) extends ObjectTransactionError

/** Indicates that the transaction could not be locked due to a conflicting transaction having already locked the object
 *  
 */
case class TransactionCollision(objectPointer: ObjectPointer, lockedTransaction: TransactionDescription) extends ObjectTransactionError

/** Indicates that the update data was not received by the DataStore
 *  
 */
case class MissingUpdateContent(objectPointer: ObjectPointer) extends ObjectTransactionError

/** Insufficient free space available to store the data associated with the transaction */
case class InsufficientFreeSpace(objectPointer: ObjectPointer) extends ObjectTransactionError

/** Indicates an operation was attempted on an object type that doesn't support it */
case class InvalidObjectType(objectPointer: ObjectPointer) extends ObjectTransactionError

/** Indicates that a KeyValue requirement was not satisfied */
case class KeyValueRequirementError(objectPointer: ObjectPointer, key: Key) extends ObjectTransactionError

/** Indicates that the transaction timestamp is less than the timestamp of the object/kv-pair */
case class TransactionTimestampError(objectPointer: ObjectPointer) extends ObjectTransactionError
