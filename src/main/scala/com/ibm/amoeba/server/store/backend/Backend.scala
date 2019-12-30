package com.ibm.amoeba.server.store.backend

import com.ibm.amoeba.common.DataBuffer
import com.ibm.amoeba.common.objects.{Metadata, ObjectId, ObjectType}
import com.ibm.amoeba.common.store.{StoreId, StorePointer}
import com.ibm.amoeba.common.transaction.TransactionId
import com.ibm.amoeba.server.store.Locater

trait Backend {
  val storeId: StoreId

  def setCompletionHandler(handler: CompletionHandler)

  /** Bootstrap-only allocation method. It cannot fail and must return a StorePointer to data committed to disk */
  def bootstrapAllocate(objectId: ObjectId,
                        objectType: ObjectType.Value,
                        metadata: Metadata,
                        data: DataBuffer): StorePointer

  def allocate(objectId: ObjectId,
               objectType: ObjectType.Value,
               metadata: Metadata,
               data: DataBuffer,
               maxSize: Option[Int]): Either[StorePointer, AllocationError.Value]

  def abortAllocation(objectId: ObjectId)

  def read(locater: Locater)

  def commit(state: CommitState, transactionId: TransactionId)
}
