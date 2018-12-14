package com.ibm.aspen.base

import java.util.UUID
import scala.concurrent.Future
import com.ibm.aspen.core.objects.ObjectPointer
import com.ibm.aspen.core.objects.ObjectRevision
import com.ibm.aspen.core.objects.ObjectRefcount
import java.nio.ByteBuffer
import scala.concurrent.ExecutionContext
import com.ibm.aspen.core.DataBuffer
import com.ibm.aspen.core.data_store.DataStoreID
import com.ibm.aspen.core.HLCTimestamp
import com.ibm.aspen.core.objects.DataObjectPointer
import com.ibm.aspen.core.objects.KeyValueObjectPointer
import com.ibm.aspen.core.objects.keyvalue.KeyValueOperation
import com.ibm.aspen.core.transaction.KeyValueUpdate

trait Transaction {
  
  val uuid: UUID
  
  def txRevision: ObjectRevision = ObjectRevision(uuid)
  
  // All returns are what the new object revision/refcount will be if the transaction completes successfully
  def append(objectPointer: DataObjectPointer, requiredRevision: ObjectRevision, data: DataBuffer): ObjectRevision
  def overwrite(objectPointer: DataObjectPointer, requiredRevision: ObjectRevision, data: DataBuffer): ObjectRevision
  
  def update(
      pointer: KeyValueObjectPointer, 
      requiredRevision: Option[ObjectRevision],
      requirements: List[KeyValueUpdate.KVRequirement],
      operations: List[KeyValueOperation]): Unit
      
  def setRefcount(objectPointer: ObjectPointer, requiredRefcount: ObjectRefcount, refcount: ObjectRefcount): ObjectRefcount
  
  /** Ensures the object revision matches the specified value and blocks other transactions attempting to modify the
   *  revision while this transaction is being executed.
   */
  def lockRevision(objectPointer: ObjectPointer, requiredRevision: ObjectRevision): Unit
  
  /** Increments the overwrite count on the object revision by 1 but leaves the object data untouched */
  def bumpVersion(objectPointer: ObjectPointer, requiredRevision: ObjectRevision): ObjectRevision
  
  def ensureHappensAfter(timestamp: HLCTimestamp): Unit
  
  def addFinalizationAction(finalizationActionUUID: UUID, serializedContent: Array[Byte]): Unit
  
  def addFinalizationAction(finalizationActionUUID: UUID): Unit
  
  def addNotifyOnResolution(storesToNotify: Set[DataStoreID]): Unit

  /** Adds a human-readable note that may be used for debugging transactions */
  def note(note: String): Unit
  
  /* Only the first error will be propagated should multiple attempts are made to invalidate the transaction
   * 
   */
  def invalidateTransaction(reason: Throwable): Unit
  
  /** True if one or more updates have been added to the transaction and it has not been invalidated */
  def valid: Boolean
  
  def result: Future[HLCTimestamp]
  
  /** Used by MissedUpdateFinalizationActions to prevent circular loops when marking objects as having missed update transactions.
   *  This method should NOT be used for any other purposes.
   * 
   */
  def disableMissedUpdateTracking(): Unit
  
  /** Sets the delay in Milliseconds after which peers will be marked as having missed the commit of the transaction
   */
  def setMissedCommitDelayInMs(msec: Int): Unit
  
  /** Begins the transaction commit process and returns a Future to its completion. This is the same future as
   *  returned by 'result' 
   *  
   *  The future successfully completes if the transaction commits. Otherwise it will fail with a TransactionError subclass.  
   */
  def commit()(implicit ec: ExecutionContext): Future[HLCTimestamp]
  
}