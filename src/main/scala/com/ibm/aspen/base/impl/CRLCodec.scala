package com.ibm.aspen.base.impl

import com.ibm.aspen.base.impl.{codec => C}
import com.google.flatbuffers.FlatBufferBuilder
import com.ibm.aspen.core.data_store.DataStoreID
import com.ibm.aspen.core.transaction.TransactionDescription
import java.nio.ByteBuffer
import com.ibm.aspen.core.transaction.TransactionDisposition
import com.ibm.aspen.core.transaction.TransactionStatus
import com.ibm.aspen.core.transaction.paxos.ProposalID
import com.ibm.aspen.core.transaction.LocalUpdate
import java.util.UUID
import com.ibm.aspen.core.DataBuffer
import com.ibm.aspen.core.allocation.AllocationRecoveryState
import com.ibm.aspen.core.HLCTimestamp
import com.ibm.aspen.core.objects.ObjectType

object CRLCodec {
  import com.ibm.aspen.core.network.NetworkCodec
  
  def encodeObjectType(e:ObjectType.Value): Byte = e match {
    case ObjectType.Data     => C.ObjectType.Data
    case ObjectType.KeyValue => C.ObjectType.KeyValue
  }
  def decodeObjectType(e: Byte): ObjectType.Value = e match {
    case C.ObjectType.Data     => ObjectType.Data
    case C.ObjectType.KeyValue => ObjectType.KeyValue
  }
  
  case class TransactionData( 
      dataStoreId: DataStoreID,
      txd: TransactionDescription,
      dataUpdateContent: Option[List[LocalUpdate]])
  
  case class TransactionState(
      disposition:TransactionDisposition.Value,
      status: TransactionStatus.Value,
      lastPromisedId: Option[ProposalID],
      lastAccepted: Option[(ProposalID, Boolean)])
      
  def toTransactionDataByteArray(storeId:DataStoreID, txd: TransactionDescription, dataUpdateContent: Option[List[LocalUpdate]]): Array[Byte] = {
    val builder = new FlatBufferBuilder(8192)
    
    val td = CRLCodec.TransactionData(storeId, txd, dataUpdateContent)
    
    val o = CRLCodec.encode(builder, td)
    
    C.CRLEntry.startCRLEntry(builder)
    C.CRLEntry.addCrlData(builder, o)
    
    val e =  C.CRLEntry.endCRLEntry(builder)
    
    builder.finish(e)
    
    builder.sizedByteArray()
  }
  def transactionDataFromByteArray(buff: Array[Byte]): TransactionData = {
    val e = C.CRLEntry.getRootAsCRLEntry(ByteBuffer.wrap(buff))
    decode(e.crlData())
  }
  
  def toTransactionStateByteArray(
      disposition:TransactionDisposition.Value,
      status: TransactionStatus.Value,
      lastPromisedId: Option[ProposalID],
      lastAccepted: Option[(ProposalID, Boolean)]): Array[Byte] = {
    val builder = new FlatBufferBuilder(1024)
    
    val ts = CRLCodec.TransactionState(disposition, status, lastPromisedId, lastAccepted)
    
    val o = CRLCodec.encode(builder, ts)
    
    C.CRLEntry.startCRLEntry(builder)
    C.CRLEntry.addCrlState(builder, o)
    
    val e =  C.CRLEntry.endCRLEntry(builder)
    
    builder.finish(e)
    
    builder.sizedByteArray()
  }
  def transactionStateFromByteArray(buff: Array[Byte]): TransactionState = {
    val e = C.CRLEntry.getRootAsCRLEntry(ByteBuffer.wrap(buff))
    decode(e.crlState())
  }
  
  def toAllocationByteArray(ars: AllocationRecoveryState): Array[Byte] = {
    val builder = new FlatBufferBuilder(8192)
    
    val o = CRLCodec.encode(builder, ars)
    
    C.CRLEntry.startCRLEntry(builder)
    C.CRLEntry.addCrlAlloc(builder, o)
    
    val e =  C.CRLEntry.endCRLEntry(builder)
    
    builder.finish(e)
    
    builder.sizedByteArray()
  }
  def allocationFromByteArray(buff: Array[Byte]): AllocationRecoveryState = {
    val e = C.CRLEntry.getRootAsCRLEntry(ByteBuffer.wrap(buff))
    decode(e.crlAlloc())
  }
  
  def encode(builder:FlatBufferBuilder, o:TransactionData): Int = {
    val dataStoreId = NetworkCodec.encode(builder, o.dataStoreId)
    val txd = NetworkCodec.encode(builder, o.txd)
    
    val (updateData, updateSizes, objectUUIDs) = o.dataUpdateContent match {
      case None => (-1, -1, -1)
      case Some(uc) => 
        
        val totalDataSize = uc.foldLeft(0)( (sz, lu) => sz + lu.data.size )
        val dbuff = builder.createUnintializedVector(1, totalDataSize, 1)
        uc.foreach(lu => dbuff.put( lu.data.asReadOnlyBuffer() ) )
        
        val updateData = builder.endVector()
        val updateSizes = C.CRLTransactionData.createUpdateSizesVector(builder, uc.map(lu => lu.data.size).toArray)
        
        val uuidArray = new Array[Byte](16 * uc.size)
        val uuidbb = ByteBuffer.wrap(uuidArray)
        
        uc.foreach(lu => {
          uuidbb.putLong(lu.objectUUID.getMostSignificantBits)
          uuidbb.putLong(lu.objectUUID.getLeastSignificantBits)
        })
        
        val objectUUIDs = C.CRLTransactionData.createUpdateUUIDsVector(builder, uuidArray)
        
        (updateData, updateSizes, objectUUIDs)
    }
    
    C.CRLTransactionData.startCRLTransactionData(builder)
    C.CRLTransactionData.addDataStoreId(builder, dataStoreId)
    C.CRLTransactionData.addTxd(builder, txd)
    
    if (updateData > 0) {
      C.CRLTransactionData.addUpdateData(builder, updateData)
      C.CRLTransactionData.addUpdateSizes(builder, updateSizes)
      C.CRLTransactionData.addUpdateUUIDs(builder, objectUUIDs)
    }
    C.CRLTransactionData.endCRLTransactionData(builder)
  }
  def decode(e: C.CRLTransactionData): TransactionData = {
    val dataStoreId = NetworkCodec.decode(e.dataStoreId())
    val txd = NetworkCodec.decode(e.txd())
    val dataUpdateContent = if (e.updateDataLength() == 0 ) {
      None
    } else {
      
      val buffs = new Array[ByteBuffer](e.updateSizesLength())
      val uuids = new Array[UUID](e.updateSizesLength())
      val dbuff = e.updateDataAsByteBuffer().asReadOnlyBuffer()
      val ubuff = e.updateUUIDsAsByteBuffer().asReadOnlyBuffer()
      var offset = dbuff.position()
      
      for (i <- 0 until e.updateSizesLength()) {
        val size = e.updateSizes(i)

        dbuff.position(offset)
        dbuff.limit(offset+size)

        buffs(i) = ByteBuffer.allocate(size)
        buffs(i).put(dbuff)
        buffs(i).position(0)
        
        val msig = ubuff.getLong()
        val lsig = ubuff.getLong()
        
        uuids(i) = new UUID(msig, lsig)
        
        offset += size
      }
      
      val localUpdates = for( i <- 0 until uuids.length) yield LocalUpdate(uuids(i), DataBuffer(buffs(i)))
      
      Some(localUpdates.toList)
    }
    
    TransactionData(dataStoreId, txd, dataUpdateContent)
  }
  
  
  def encode(builder:FlatBufferBuilder, o:TransactionState): Int = {
    val disposition = NetworkCodec.encodeTransactionDisposition(o.disposition)
    val status = NetworkCodec.encodeTransactionStatus(o.status)
    
    C.CRLTransactionState.startCRLTransactionState(builder)
    C.CRLTransactionState.addDisposition(builder, disposition)
    C.CRLTransactionState.addStatus(builder, status)
    o.lastPromisedId.foreach( pid => C.CRLTransactionState.addLastPromisedId(builder, NetworkCodec.encode(builder, pid)) )
    o.lastAccepted.foreach(t => { 
      C.CRLTransactionState.addLastAcceptedId(builder, NetworkCodec.encode(builder, t._1))
      C.CRLTransactionState.addLastAcceptedValue(builder, t._2)
    })
    
    C.CRLTransactionState.endCRLTransactionState(builder)
  }
  def decode(e: C.CRLTransactionState): TransactionState = {
    val disposition = NetworkCodec.decodeTransactionDispositione(e.disposition())
    val status = NetworkCodec.decodeTransactionStatus(e.status())
    
    val lastPromisedId = if (e.lastPromisedId() == null)
      None
    else
      Some(NetworkCodec.decode(e.lastPromisedId()))
      
    val lastAccepted = if(e.lastAcceptedId() == null)
      None
    else
      Some((NetworkCodec.decode(e.lastAcceptedId()), e.lastAcceptedValue()))
    
    TransactionState(disposition, status, lastPromisedId, lastAccepted)
  }
  
  def encode(builder:FlatBufferBuilder, o:AllocationRecoveryState.NewObject): Int = {
    val storePointer = NetworkCodec.encode(builder, o.storePointer)
    builder.createUnintializedVector(1, o.objectData.size, 1).put(o.objectData.asReadOnlyBuffer())
    val objectData = builder.endVector()
    
    C.CRLNewObject.startCRLNewObject(builder)
    C.CRLNewObject.addStorePointer(builder, storePointer)
    C.CRLNewObject.addNewObjectUUID(builder, NetworkCodec.encode(builder, o.newObjectUUID))
    C.CRLNewObject.addObjectType(builder, encodeObjectType(o.objectType))
    C.CRLNewObject.addObjectSize(builder, o.objectSize.getOrElse(0))
    C.CRLNewObject.addObjectData(builder, objectData)
    C.CRLNewObject.addInitialRefcount(builder, NetworkCodec.encode(builder, o.initialRefcount))
    C.CRLNewObject.endCRLNewObject(builder)
  }
  def decode(e: C.CRLNewObject): AllocationRecoveryState.NewObject = {
    
    val storePointer = NetworkCodec.decode(e.storePointer())
    val newObjectUUID = NetworkCodec.decode(e.newObjectUUID())
    val objectType = decodeObjectType(e.objectType())
    val objectSize = if (e.objectSize() == 0) None else Some(e.objectSize())
    val data = ByteBuffer.allocate(e.objectDataLength())
    data.put(e.objectDataAsByteBuffer().asReadOnlyBuffer())
    data.position(0)
    val initialRefcount = NetworkCodec.decode(e.initialRefcount())
    
    AllocationRecoveryState.NewObject(storePointer, newObjectUUID, objectType, objectSize, DataBuffer(data), initialRefcount)
  }
  
  def encode(builder:FlatBufferBuilder, o:AllocationRecoveryState): Int = {
    val storeId = NetworkCodec.encode(builder, o.storeId)
    val newObjects = C.CRLAllocationRecoveryState.createNewObjectsVector(builder, o.newObjects.map(encode(builder, _)).toArray)
    val allocatingObject = NetworkCodec.encode(builder, o.allocatingObject)
    
    
    C.CRLAllocationRecoveryState.startCRLAllocationRecoveryState(builder)
    C.CRLAllocationRecoveryState.addDataStoreID(builder, storeId)
    C.CRLAllocationRecoveryState.addNewObjects(builder, newObjects)
    C.CRLAllocationRecoveryState.addTimestamp(builder, o.timestamp.asLong)
    C.CRLAllocationRecoveryState.addAllocationTransactionUUID(builder, NetworkCodec.encode(builder, o.allocationTransactionUUID))
    C.CRLAllocationRecoveryState.addAllocatingObject(builder, allocatingObject)
    C.CRLAllocationRecoveryState.addAllocatingObjectRevision(builder, NetworkCodec.encodeObjectRevision(builder, o.allocatingObjectRevision))
    C.CRLAllocationRecoveryState.endCRLAllocationRecoveryState(builder)
  }
  def decode(e: C.CRLAllocationRecoveryState): AllocationRecoveryState = {
    val dataStoreId = NetworkCodec.decode(e.dataStoreID())
    val allocationTransactionUUID = NetworkCodec.decode(e.allocationTransactionUUID())
    val allocatingObject = NetworkCodec.decode(e.allocatingObject())
    val allocatingObjectRevision = NetworkCodec.decode(e.allocatingObjectRevision())
    val timestamp = HLCTimestamp(e.timestamp())
    
    def newObjects(idx: Int, l:List[AllocationRecoveryState.NewObject]): List[AllocationRecoveryState.NewObject] = if (idx == -1)
        l
      else
        newObjects(idx-1, decode(e.newObjects(idx)) :: l)
    
    AllocationRecoveryState(dataStoreId, newObjects(e.newObjectsLength()-1, Nil), timestamp, allocationTransactionUUID, allocatingObject, allocatingObjectRevision)
  }
}