package com.ibm.aspen.core.transaction

import java.util.UUID
import com.ibm.aspen.core.objects.ObjectPointer

final case class TransactionDescription(
  
  /** Uniquely identifies this transaction */
  transactionUUID: UUID,
  
  /** Used for graceful transaction contention handling.
   *  
   *  DO NOT rely on this value being in any way accurate. This value is not protected
   *  against clock skew, drift, system-clock changes, etc. Just don't use it. Pretend
   *  it doesn't exist.
   */
  startTimestamp: Long,
  
  /** Defines the primary object which is used for identifying the peers and quorum threshold used to resolve the transaction.
   *
   * Multiple objects in different pools may be modified by the transaction but only one
   * object is used to define the Paxos quorum used to actually resolve the commit/abort
   * decision. This must be set to the object with the strictest reliability constraints
   * from amongst all of the objects modified by the transaction.
   */
  primaryObject: ObjectPointer,
  
  /** Specifies the peer within the primary pool responsible for driving the transaction to closure 
   * 
   * The "Prepare" message may be sent by a non-member of the primary pool but the designated leader
   * is responsible for performing the role of the Paxos Proposer. It's also responsible for ensuring
   * that the list of FinalizationActions are executed. This peer may die or be initially unavailable
   * and therefore require a new leader to be elected but by specifying this up front, we avoid
   * leadership battles that would otherwise be required for every transaction.
   */
  designatedLeaderUID: Byte,
  
  dataUpdates: List[DataUpdate],
  refcountUpdates: List[RefcountUpdate],
  finalizationActions: List[SerializedFinalizationAction]) {

  def allReferencedObjectsSet = (dataUpdates.iterator.map(_.objectPointer) ++ refcountUpdates.iterator.map(_.objectPointer)).toSet
  
}