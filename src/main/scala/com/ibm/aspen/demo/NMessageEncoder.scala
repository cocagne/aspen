package com.ibm.aspen.demo

import java.nio.ByteBuffer

import com.google.flatbuffers.FlatBufferBuilder
import com.ibm.aspen.core.allocation.{Allocate, AllocateResponse, Message => AllocationMessage}
import com.ibm.aspen.core.data_store.ObjectMetadata
import com.ibm.aspen.core.network.NetworkCodec
import com.ibm.aspen.core.network.protocol.Message
import com.ibm.aspen.core.read.{OpportunisticRebuild, Read, ReadResponse, TransactionCompletionQuery, TransactionCompletionResponse, Message => ReadMessage}
import com.ibm.aspen.core.transaction.{TransactionData, TxAccept, TxAcceptResponse, TxCommitted, TxFinalized, TxHeartbeat, TxPrepare, TxPrepareResponse, TxResolved, TxStatusRequest, TxStatusResponse, Message => TransactionMessage}

object NMessageEncoder {
  
  def encodeMessage(message: TransactionMessage, updateContent: Option[TransactionData] = None): Array[Byte] = {
    val builder = new FlatBufferBuilder(4096)
    
    val encodedMsg = message match {
      case m: TxPrepare =>
        val o = NetworkCodec.encode(builder, m)
    
        Message.startMessage(builder)
        Message.addPrepare(builder, o)

        Message.finishMessageBuffer(builder, Message.endMessage(builder))
        
        builder.sizedByteArray()

      case m: TxPrepareResponse =>
        val o = NetworkCodec.encode(builder, m)
    
        Message.startMessage(builder)
        Message.addPrepareResponse(builder, o)

        Message.finishMessageBuffer(builder, Message.endMessage(builder))
        
        builder.sizedByteArray()
        
      case m: TxAccept =>
        val o = NetworkCodec.encode(builder, m)
    
        Message.startMessage(builder)
        Message.addAccept(builder, o)

        Message.finishMessageBuffer(builder, Message.endMessage(builder))
        
        builder.sizedByteArray()
        
      case m: TxAcceptResponse =>
        val o = NetworkCodec.encode(builder, m)
    
        Message.startMessage(builder)
        Message.addAcceptResponse(builder, o)

        Message.finishMessageBuffer(builder, Message.endMessage(builder))
        
        builder.sizedByteArray()
        
      case m: TxResolved =>
        val o = NetworkCodec.encode(builder, m)
    
        Message.startMessage(builder)
        Message.addResolved(builder, o)

        Message.finishMessageBuffer(builder, Message.endMessage(builder))
        
        builder.sizedByteArray()
        
      case m: TxCommitted =>
        val o = NetworkCodec.encode(builder, m)
    
        Message.startMessage(builder)
        Message.addCommitted(builder, o)

        Message.finishMessageBuffer(builder, Message.endMessage(builder))
        
        builder.sizedByteArray()
        
      case m: TxFinalized =>
        val o = NetworkCodec.encode(builder, m)
    
        Message.startMessage(builder)
        Message.addFinalized(builder, o)

        Message.finishMessageBuffer(builder, Message.endMessage(builder))
        
        builder.sizedByteArray()
        
      case m: TxHeartbeat =>
        val o = NetworkCodec.encode(builder, m)
    
        Message.startMessage(builder)
        Message.addHeartbeat(builder, o)

        Message.finishMessageBuffer(builder, Message.endMessage(builder))
        
        builder.sizedByteArray()

      case m: TxStatusRequest =>
        val o = NetworkCodec.encode(builder, m)

        Message.startMessage(builder)
        Message.addHeartbeat(builder, o)

        Message.finishMessageBuffer(builder, Message.endMessage(builder))

        builder.sizedByteArray()

      case m: TxStatusResponse =>
        val o = NetworkCodec.encode(builder, m)

        Message.startMessage(builder)
        Message.addHeartbeat(builder, o)

        Message.finishMessageBuffer(builder, Message.endMessage(builder))

        builder.sizedByteArray()
    }
    
    val (contentSize, preTxSize) = updateContent match {
      case None => (0, 0)
      case Some(td) =>
        val luSize = td.localUpdates.foldLeft(0)((sz, lu) => sz + 16 + 4 + lu.data.size)
        val lpSize = td.preTransactionRebuilds.foldLeft(0)((sz, p) => sz + 16 + ObjectMetadata.EncodedSize + 4 + p.data.size)

        (luSize, lpSize)
    }
    
    val msg = new Array[Byte](4 + encodedMsg.length + 4 + 4 + contentSize + preTxSize)
    val bb = ByteBuffer.wrap(msg)
    
    bb.putInt(encodedMsg.length)
    bb.put(encodedMsg)
    bb.putInt(contentSize)
    bb.putInt(preTxSize)
   
    updateContent.foreach { td =>

      td.localUpdates.foreach { lu =>
        bb.putLong(lu.objectUUID.getMostSignificantBits)
        bb.putLong(lu.objectUUID.getLeastSignificantBits)
        bb.putInt(lu.data.size)
        bb.put(lu.data.asReadOnlyBuffer())
      }

      td.preTransactionRebuilds.foreach { pt =>
        bb.putLong(pt.objectUUID.getMostSignificantBits)
        bb.putLong(pt.objectUUID.getLeastSignificantBits)
        pt.metadata.encodeInto(bb)
        bb.putInt(pt.data.size)
        bb.put(pt.data.asReadOnlyBuffer())
      }
    }
    
    msg
  }
  
  def encodeMessage(message: AllocationMessage): Array[Byte] = {
    val builder = new FlatBufferBuilder(4096)
    
    val encodedMsg = message match {
      case m: Allocate =>
        val o = NetworkCodec.encode(builder, m)
    
        Message.startMessage(builder)
        Message.addAllocate(builder, o)

        Message.finishMessageBuffer(builder, Message.endMessage(builder))
        
        builder.sizedByteArray()
        
      case m: AllocateResponse =>
        val o = NetworkCodec.encode(builder, m)
    
        Message.startMessage(builder)
        Message.addAllocateResponse(builder, o)

        Message.finishMessageBuffer(builder, Message.endMessage(builder))
        
        builder.sizedByteArray()
    }
    
    val msg = new Array[Byte](4 + encodedMsg.length)
    val bb = ByteBuffer.wrap(msg)
    
    bb.putInt(encodedMsg.length)
    bb.put(encodedMsg)

    msg
  }
  
  def encodeMessage(message: ReadMessage): Array[Byte] = {
    val builder = new FlatBufferBuilder(4096)
    
    val encodedMsg = message match {
      case m: Read =>
        val o = NetworkCodec.encode(builder, m)
    
        Message.startMessage(builder)
        Message.addRead(builder, o)

        Message.finishMessageBuffer(builder, Message.endMessage(builder))
        
        builder.sizedByteArray()
        
      case m: ReadResponse =>
        val o = NetworkCodec.encode(builder, m)
    
        Message.startMessage(builder)
        Message.addReadResponse(builder, o)

        Message.finishMessageBuffer(builder, Message.endMessage(builder))
        
        builder.sizedByteArray()
        
      case m: OpportunisticRebuild =>
        val o = NetworkCodec.encode(builder, m)
    
        Message.startMessage(builder)
        Message.addOpportunisticRebuild(builder, o)

        Message.finishMessageBuffer(builder, Message.endMessage(builder))
        
        builder.sizedByteArray()

      case m: TransactionCompletionQuery =>
        val o = NetworkCodec.encode(builder, m)

        Message.startMessage(builder)
        Message.addTransactionCompletionQuery(builder, o)

        Message.finishMessageBuffer(builder, Message.endMessage(builder))

        builder.sizedByteArray()

      case m: TransactionCompletionResponse =>
        val o = NetworkCodec.encode(builder, m)

        Message.startMessage(builder)
        Message.addTransactionCompletionResponse(builder, o)

        Message.finishMessageBuffer(builder, Message.endMessage(builder))

        builder.sizedByteArray()
    }
    
    val msg = new Array[Byte](4 + encodedMsg.length)
    val bb = ByteBuffer.wrap(msg)
    
    bb.putInt(encodedMsg.length)
    bb.put(encodedMsg)

    msg
  }
}