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

sealed abstract class Message

final case class Read(
    toStore: DataStoreID,
    fromClient: ClientID,
    readUUID: UUID,
    objectPointer: ObjectPointer,
    readType: ReadType,
    returnLockedTransaction: Boolean = false) extends Message
    
final case class ReadResponse(
    fromStore: DataStoreID,
    readUUID: UUID,
    result: Either[ReadError.Value, ReadResponse.CurrentState]) extends Message
    
object ReadResponse {
  case class CurrentState(
      revision: ObjectRevision,
      updates: Set[UUID],
      refcount: ObjectRefcount,
      timestamp: HLCTimestamp,
      objectData: Option[DataBuffer],
      locks: List[Lock]) {
    
    override def equals(other: Any): Boolean = other match {
      case rhs: CurrentState =>
        
        val dmatch = (objectData, rhs.objectData) match {
          case (Some(lhs), Some(rhs)) => lhs.compareTo(rhs) == 0
          case (None, None) => true
          case _ => false
        }
        
        revision == rhs.revision && refcount == rhs.refcount && dmatch && updates == rhs.updates && locks == rhs.locks
        
      case _ => false
    }
  }
}