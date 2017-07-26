package com.ibm.aspen.base.impl

import com.ibm.aspen.base.impl.{crl => C}
import com.google.flatbuffers.FlatBufferBuilder
import com.ibm.aspen.core.data_store.DataStoreID
import com.ibm.aspen.core.transaction.TransactionDescription
import java.nio.ByteBuffer
import com.ibm.aspen.core.transaction.TransactionDisposition
import com.ibm.aspen.core.transaction.TransactionStatus
import com.ibm.aspen.core.transaction.paxos.ProposalID

object CRLCodec {
  import com.ibm.aspen.core.network.{Codec => NetCodec}
  
  case class TransactionData( 
      dataStoreId: DataStoreID,
      txd: TransactionDescription,
      dataUpdateContent: Option[Array[ByteBuffer]])
  
  case class TransactionState(
      disposition:TransactionDisposition.Value,
      status: TransactionStatus.Value,
      lastPromisedId: Option[ProposalID],
      lastAccepted: Option[(ProposalID, Boolean)])
      
  def toTransactionDataByteArray(storeId:DataStoreID, txd: TransactionDescription, dataUpdateContent: Option[Array[ByteBuffer]]): Array[Byte] = {
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
  
  def encode(builder:FlatBufferBuilder, o:TransactionData): Int = {
    val dataStoreId = NetCodec.encode(builder, o.dataStoreId)
    val txd = NetCodec.encode(builder, o.txd)
    
    val (updateData, updateSizes) = o.dataUpdateContent match {
      case None => (-1, -1)
      case Some(uc) => 
        val totalDataSize = uc.foldLeft(0)( (sz, bb) => sz + bb.capacity )
        val dbuff = builder.createUnintializedVector(1, totalDataSize, 1)
        uc.foreach(db => dbuff.put( db.asReadOnlyBuffer() ) )
        
        val updateData = builder.endVector()
        val updateSizes = C.CRLTransactionData.createUpdateSizesVector(builder, uc.map( bb => bb.capacity ))
        (updateData, updateSizes)
    }
    
    C.CRLTransactionData.startCRLTransactionData(builder)
    C.CRLTransactionData.addDataStoreId(builder, dataStoreId)
    C.CRLTransactionData.addTxd(builder, txd)
    
    if (updateData > 0) {
      C.CRLTransactionData.addUpdateData(builder, updateData)
      C.CRLTransactionData.addUpdateSizes(builder, updateSizes)
    }
    C.CRLTransactionData.endCRLTransactionData(builder)
  }
  def decode(e: C.CRLTransactionData): TransactionData = {
    val dataStoreId = NetCodec.decode(e.dataStoreId())
    val txd = NetCodec.decode(e.txd())
    val dataUpdateContent = if (e.updateDataLength() == 0 ) {
      None
    } else {
      
      val buffs = new Array[ByteBuffer](e.updateSizesLength())
      val dbuff = e.updateDataAsByteBuffer()
      var offset = dbuff.position()
      
      for (i <- 0 until e.updateSizesLength()) {
        val size = e.updateSizes(i)

        dbuff.position(offset)
        dbuff.limit(offset+size)

        buffs(i) = ByteBuffer.allocate(size)
        buffs(i).put(dbuff)
        buffs(i).position(0)
        offset += size
      }
      
      Some(buffs)
    }
    
    TransactionData(dataStoreId, txd, dataUpdateContent)
  }
  
  
  def encode(builder:FlatBufferBuilder, o:TransactionState): Int = {
    val disposition = NetCodec.encodeTransactionDisposition(o.disposition)
    val status = NetCodec.encodeTransactionStatus(o.status)
    
    C.CRLTransactionState.startCRLTransactionState(builder)
    C.CRLTransactionState.addDisposition(builder, disposition)
    C.CRLTransactionState.addStatus(builder, status)
    o.lastPromisedId.foreach( pid => C.CRLTransactionState.addLastPromisedId(builder, NetCodec.encode(builder, pid)) )
    o.lastAccepted.foreach(t => { 
      C.CRLTransactionState.addLastAcceptedId(builder, NetCodec.encode(builder, t._1))
      C.CRLTransactionState.addLastAcceptedValue(builder, t._2)
    })
    
    C.CRLTransactionState.endCRLTransactionState(builder)
  }
  def decode(e: C.CRLTransactionState): TransactionState = {
    val disposition = NetCodec.decodeTransactionDispositione(e.disposition())
    val status = NetCodec.decodeTransactionStatus(e.status())
    
    val lastPromisedId = if (e.lastPromisedId() == null)
      None
    else
      Some(NetCodec.decode(e.lastPromisedId()))
      
    val lastAccepted = if(e.lastAcceptedId() == null)
      None
    else
      Some((NetCodec.decode(e.lastAcceptedId()), e.lastAcceptedValue()))
    
    TransactionState(disposition, status, lastPromisedId, lastAccepted)
  }
}