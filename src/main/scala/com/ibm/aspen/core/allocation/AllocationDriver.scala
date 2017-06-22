package com.ibm.aspen.core.allocation

import com.ibm.aspen.core.network.TransactionMessenger
import java.util.UUID
import scala.concurrent.Promise
import com.ibm.aspen.core.objects.ObjectPointer
import com.ibm.aspen.core.objects.ObjectRefcount
import com.ibm.aspen.core.objects.ObjectRevision

abstract class AllocationDriver (
    val messenger: TransactionMessenger,
    val poolUUID: UUID,
    val newObjectUUID: UUID,
    val objectSize: Option[Int],
    val objectData: Map[Byte,Array[Byte]], // Map DataStore pool index -> store-specific ObjectData
    val initialRefcount: ObjectRefcount,
    val allocationTransactionUUID: UUID,
    val allocatingObject: ObjectPointer,
    val allocatingObjectRevision: ObjectRevision
    ){
  
  private[this] val promise = Promise[ObjectPointer]
  
  def futureResult = promise.future
}

object AllocationDriver {
  trait Factory {
    def create(): AllocationDriver
  }
}