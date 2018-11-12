package com.ibm.aspen.core.read

import com.ibm.aspen.core.data_store.DataStoreID
import com.ibm.aspen.core.network.ClientID
import com.ibm.aspen.core.objects.ObjectPointer
import java.util.UUID
import com.ibm.aspen.core.objects.ObjectRevision
import com.ibm.aspen.core.objects.ObjectRefcount
import com.ibm.aspen.core.transaction.TransactionDescription
import java.nio.ByteBuffer
import com.ibm.aspen.core.DataBuffer
import com.ibm.aspen.core.HLCTimestamp
import com.ibm.aspen.core.data_store.Lock
import com.ibm.aspen.core.data_store.ObjectReadError
import com.ibm.aspen.core.objects.keyvalue.Key
import com.ibm.aspen.util.Varint

sealed abstract class Message

final case class Read(
    toStore: DataStoreID,
    fromClient: ClientID,
    readUUID: UUID,
    objectPointer: ObjectPointer,
    readType: ReadType) extends Message
    
final case class ReadResponse(
    fromStore: DataStoreID,
    readUUID: UUID,
    readTime: HLCTimestamp,
    result: Either[ObjectReadError.Value, ReadResponse.CurrentState]) extends Message
    


object ReadResponse {
  case class CurrentState(
      revision: ObjectRevision,
      refcount: ObjectRefcount,
      timestamp: HLCTimestamp,
      sizeOnStore: Int,
      objectData: Option[DataBuffer],
      lockedWriteTransactions: Set[UUID]) {
    
    override def equals(other: Any): Boolean = other match {
      case rhs: CurrentState =>
        
        val dmatch = (objectData, rhs.objectData) match {
          case (Some(lhs), Some(rhs)) => lhs.compareTo(rhs) == 0
          case (None, None) => true
          case _ => false
        }
        
        revision == rhs.revision && refcount == rhs.refcount && dmatch && lockedWriteTransactions == rhs.lockedWriteTransactions
        
      case _ => false
    }
  }
}

final case class OpportunisticRebuild(
                                       toStore: DataStoreID,
                                       fromClient: ClientID,
                                       pointer: ObjectPointer,
                                       revision: ObjectRevision,
                                       refcount: ObjectRefcount,
                                       timestamp: HLCTimestamp,
                                       data: DataBuffer) extends Message

final case class TransactionCompletionQuery(
                                             toStore: DataStoreID,
                                             fromClient: ClientID,
                                             queryUUID: UUID,
                                             transactionUUID: UUID) extends Message

final case class TransactionCompletionResponse(
                                           fromStore: DataStoreID,
                                           queryUUID: UUID,
                                           isComplete: Boolean) extends Message