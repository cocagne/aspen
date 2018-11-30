package com.ibm.aspen.core.transaction

import java.util.UUID
import com.ibm.aspen.core.objects.ObjectPointer
import com.ibm.aspen.core.network.ClientID
import com.ibm.aspen.core.data_store.DataStoreID

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
  
  requirements: List[TransactionRequirement],
  
  finalizationActions: List[SerializedFinalizationAction],
  
  /** Specifies which client initiated the transaction. Transaction resolution messages will be sent
   *  here as well as to the participating data stores.
   */
  originatingClient: Option[ClientID] = None,
  
  /** Specifies an additional set of stores to receive transaction resolution notices. Primary use case
   *  is for notifying stores of the result of an object allocation attempt.
   */
  notifyOnResolution: List[DataStoreID] = Nil ) {

  def allReferencedObjectsSet = requirements.map(_.objectPointer).toSet
  
  def primaryObjectDataStores: Set[DataStoreID] = primaryObject.storePointers.foldLeft(Set[DataStoreID]())((s, sp) => s + DataStoreID(primaryObject.poolUUID, sp.poolIndex))
  
  def allDataStores = allReferencedObjectsSet.flatMap(ptr => ptr.storePointers.map(sp => DataStoreID(ptr.poolUUID, sp.poolIndex)))
  
  def allHostedObjects(storeId: DataStoreID): List[ObjectPointer] = allReferencedObjectsSet.foldLeft(List[ObjectPointer]())((l, op) => {
      if (op.poolUUID == storeId.poolUUID) {
        op.storePointers.find(_.poolIndex == storeId.poolIndex) match {
          case Some(sp) => op :: l
          case None => l
        }
      } else
        l
    })

  def shortString: String = {
    val ol = allReferencedObjectsSet.map(_.shortString).toList.sorted
    s"Tx $transactionUUID: Objects: $ol"
  }
}