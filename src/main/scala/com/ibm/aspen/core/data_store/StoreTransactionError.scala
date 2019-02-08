package com.ibm.aspen.core.data_store

import com.ibm.aspen.core.HLCTimestamp
import com.ibm.aspen.core.objects.{ObjectPointer, ObjectRefcount, ObjectRevision}
import com.ibm.aspen.core.objects.keyvalue.Key
import com.ibm.aspen.core.transaction.{LocalTimeRequirement, TransactionDescription}

sealed abstract class StoreTransactionError

case class LocalTimeError(timestamp: HLCTimestamp,
                          requirement: LocalTimeRequirement.Requirement.Value,
                          localtime: HLCTimestamp) extends StoreTransactionError


sealed abstract class ObjectTransactionError extends StoreTransactionError {
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
case class RevisionMismatch(objectPointer: ObjectPointer, required: ObjectRevision, current: ObjectRevision) extends ObjectTransactionError {
  override def toString: String = s"RevisionMismatch(${objectPointer.shortString}, Required: $required, current: $current)"
}

/** Returned during DataStore.lockOrCollide if the refcount of the object changed between the initial state
  *  query and the time at which the lock operation was preformed
  *
  */
case class RefcountMismatch(objectPointer: ObjectPointer, required: ObjectRefcount, current: ObjectRefcount) extends ObjectTransactionError {
  override def toString: String = s"RefcountMismatch(${objectPointer.shortString}, Required: $required, current: $current)"
}

/** Indicates that the transaction could not be locked due to a conflicting transaction having already locked the object
  *
  */
case class TransactionCollision(objectPointer: ObjectPointer,
                                lockedTransaction: TransactionDescription,
                                requiredRevision: Option[ObjectRevision]) extends ObjectTransactionError {
  override def toString: String = {
    s"TransactionCollision(${objectPointer.shortString}, Tx:${lockedTransaction.transactionUUID}, requiredRev:$requiredRevision)"
  }
}

/** Indicates that the update data was not received by the DataStore
  *
  */
case class MissingUpdateContent(objectPointer: ObjectPointer) extends ObjectTransactionError

/** Insufficient free space available to store the data associated with the transaction */
case class InsufficientFreeSpace(objectPointer: ObjectPointer) extends ObjectTransactionError

/** Indicates an operation was attempted on an object type that doesn't support it */
case class InvalidObjectType(objectPointer: ObjectPointer) extends ObjectTransactionError

/** Indicates that a KeyValue requirement was not satisfied */
case class KeyValueRequirementError(objectPointer: ObjectPointer, key: Key, orevision: Option[ObjectRevision]) extends ObjectTransactionError {
  override def toString: String = s"KeyValueRequirementError(${objectPointer.shortString}, key:$key, currentRevision:$orevision)"
}

/** Indicates that the transaction timestamp is less than the timestamp of the object/kv-pair */
case class TransactionTimestampError(objectPointer: ObjectPointer) extends ObjectTransactionError {
  override def toString: String = s"TransactionTimestampError(${objectPointer.shortString})"
}

/** Indicates that the transaction timestamp is less than the timestamp of the object/kv-pair */
case class RebuildCollision(objectPointer: ObjectPointer) extends ObjectTransactionError
