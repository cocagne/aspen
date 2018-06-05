package com.ibm.aspen.demo

import com.ibm.aspen.core.data_store.DataStoreID
import com.ibm.aspen.core.network.protocol.Message
import java.nio.ByteBuffer
import com.ibm.aspen.core.network.NetworkCodec
import com.ibm.aspen.core.read
import com.ibm.aspen.core.allocation
import com.ibm.aspen.core.network.ClientID
import com.ibm.aspen.core.transaction.TxAcceptResponse
import com.ibm.aspen.core.transaction.TxResolved
import com.ibm.aspen.core.transaction.TxFinalized
import com.ibm.aspen.core.transaction.TxPrepare
import com.ibm.aspen.core.transaction.LocalUpdate
import com.ibm.aspen.core.allocation.AllocationStatusRequest
import com.ibm.aspen.core.allocation.AllocationStatusReply
import java.util.UUID
import com.ibm.aspen.core.DataBuffer
import com.google.flatbuffers.FlatBufferBuilder
import com.ibm.aspen.core.transaction.TxPrepareResponse
import com.ibm.aspen.core.transaction.TxAccept
import com.ibm.aspen.core.transaction.TxHeartbeat
import com.ibm.aspen.util.uuid2byte
import com.ibm.aspen.core.transaction.{Message => TransactionMessage}
import com.ibm.aspen.core.allocation.{Message => AllocationMessage}
import com.ibm.aspen.core.read.{Message => ReadMessage}
import com.ibm.aspen.core.allocation.Allocate
import com.ibm.aspen.core.allocation.AllocateResponse
import com.ibm.aspen.core.read.Read
import com.ibm.aspen.core.read.ReadResponse
import org.zeromq.ZMsg

object MessageEncoder {
  
  def encodePrepare(message: TxPrepare, updateContent: Option[List[LocalUpdate]]): ZMsg = {
    val zmsg = encodeMessage(None, message)
    updateContent.foreach { l =>
      val sz = l.foldLeft(0)((sz, lu) => sz + 16 + 4 + lu.data.size)
      if (sz > 0) {
        val arr = new Array[Byte](sz)
        val bb = ByteBuffer.wrap(arr)
        l.foreach { lu =>
          bb.putLong(lu.objectUUID.getMostSignificantBits)
          bb.putLong(lu.objectUUID.getLeastSignificantBits)
          bb.putInt(lu.data.size)
          bb.put(lu.data.asReadOnlyBuffer())
        }
        zmsg.add(arr)
      }
    }
    zmsg
  }
  
  def encodeMessage(oclient: Option[ClientID], message: TransactionMessage): ZMsg = {
    val builder = new FlatBufferBuilder(4096)
    val zmsg = new ZMsg()
    
    oclient.foreach(clientId => zmsg.add(uuid2byte(clientId.uuid)))
    
    message match {
      case m: TxPrepare =>
        val o = NetworkCodec.encode(builder, m)
    
        Message.startMessage(builder)
        Message.addReadResponse(builder, o)

        builder.finish(Message.endMessage(builder))
        
        zmsg.add(builder.dataBuffer().array())

      case m: TxPrepareResponse =>
        val o = NetworkCodec.encode(builder, m)
    
        Message.startMessage(builder)
        Message.addPrepareResponse(builder, o)

        builder.finish(Message.endMessage(builder))
        
        zmsg.add(builder.dataBuffer().array())
        
      case m: TxAccept =>
        val o = NetworkCodec.encode(builder, m)
    
        Message.startMessage(builder)
        Message.addAccept(builder, o)

        builder.finish(Message.endMessage(builder))
        
        zmsg.add(builder.dataBuffer().array())
        
      case m: TxAcceptResponse =>
        val o = NetworkCodec.encode(builder, m)
    
        Message.startMessage(builder)
        Message.addAcceptResponse(builder, o)

        builder.finish(Message.endMessage(builder))
        
        zmsg.add(builder.dataBuffer().array())
        
      case m: TxResolved =>
        val o = NetworkCodec.encode(builder, m)
    
        Message.startMessage(builder)
        Message.addResolved(builder, o)

        builder.finish(Message.endMessage(builder))
        
        zmsg.add(builder.dataBuffer().array())
        
      case m: TxFinalized =>
        val o = NetworkCodec.encode(builder, m)
    
        Message.startMessage(builder)
        Message.addFinalized(builder, o)

        builder.finish(Message.endMessage(builder))
        
        zmsg.add(builder.dataBuffer().array())
        
      case m: TxHeartbeat =>
        val o = NetworkCodec.encode(builder, m)
    
        Message.startMessage(builder)
        Message.addHeartbeat(builder, o)

        builder.finish(Message.endMessage(builder))
        
        zmsg.add(builder.dataBuffer().array())
    }
    
    zmsg
  }
  
  def encodeMessage(oclient: Option[ClientID], message: AllocationMessage): ZMsg = {
    val builder = new FlatBufferBuilder(4096)
    val zmsg = new ZMsg()
    
    oclient.foreach(clientId => zmsg.add(uuid2byte(clientId.uuid)))
    
    message match {
      case m: Allocate =>
        val o = NetworkCodec.encode(builder, m)
    
        Message.startMessage(builder)
        Message.addAllocate(builder, o)

        builder.finish(Message.endMessage(builder))
        
        zmsg.add(builder.dataBuffer().array())
        
      case m: AllocateResponse =>
        val o = NetworkCodec.encode(builder, m)
    
        Message.startMessage(builder)
        Message.addAllocateResponse(builder, o)

        builder.finish(Message.endMessage(builder))
        
        zmsg.add(builder.dataBuffer().array())
        
      case m: AllocationStatusRequest =>
        val o = NetworkCodec.encode(builder, m)
    
        Message.startMessage(builder)
        Message.addAllocateStatus(builder, o)

        builder.finish(Message.endMessage(builder))
        
        zmsg.add(builder.dataBuffer().array())
        
      case m: AllocationStatusReply =>
        val o = NetworkCodec.encode(builder, m)
    
        Message.startMessage(builder)
        Message.addAllocateStatusResponse(builder, o)

        builder.finish(Message.endMessage(builder))
        
        zmsg.add(builder.dataBuffer().array())
    }
    
    zmsg
  }
  
  def encodeMessage(oclient: Option[ClientID], message: ReadMessage): ZMsg = {
    val builder = new FlatBufferBuilder(4096)
    val zmsg = new ZMsg()
    
    oclient.foreach(clientId => zmsg.add(uuid2byte(clientId.uuid)))
    
    message match {
      case m: Read =>
        val o = NetworkCodec.encode(builder, m)
    
        Message.startMessage(builder)
        Message.addRead(builder, o)

        builder.finish(Message.endMessage(builder))
        
        zmsg.add(builder.dataBuffer().array())
        
      case m: ReadResponse =>
        val o = NetworkCodec.encode(builder, m)
    
        Message.startMessage(builder)
        Message.addReadResponse(builder, o)

        builder.finish(Message.endMessage(builder))
        
        zmsg.add(builder.dataBuffer().array())
    }
    
    zmsg
  }
}