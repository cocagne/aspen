package com.ibm.aspen.fuse.protocol

import java.nio.ByteBuffer
import java.nio.channels.FileChannel
import java.nio.ByteOrder
import scala.annotation.tailrec
import com.ibm.aspen.fuse.FuseOptions
import com.ibm.aspen.fuse.LinuxAPI
import com.ibm.aspen.fuse.protocol.OpCode._
import com.ibm.aspen.fuse.protocol.messages.InitReply
import com.ibm.aspen.fuse.protocol.messages.InitRequest
import com.ibm.aspen.fuse.protocol.messages.ReadDirRequest
import com.ibm.aspen.fuse.protocol.messages.RequestHeader
import com.ibm.aspen.fuse.protocol.messages.GetAttrRequest
import com.ibm.aspen.fuse.protocol.messages.OpenDirRequest
import com.ibm.aspen.fuse.protocol.messages.ReleaseRequest
import com.ibm.aspen.fuse.protocol.messages.ReleaseDirRequest
import com.ibm.aspen.fuse.protocol.messages.LookupRequest
import com.ibm.aspen.fuse.protocol.messages.OpenRequest
import com.ibm.aspen.fuse.protocol.messages.ReadRequest
import java.nio.channels.GatheringByteChannel
import java.nio.channels.ScatteringByteChannel
import com.ibm.aspen.fuse.protocol.messages.WriteRequest
import com.ibm.aspen.fuse.protocol.messages.SetAttrRequest
import com.ibm.aspen.fuse.protocol.messages.MknodRequest
import com.ibm.aspen.fuse.protocol.messages.RenameRequest
import com.ibm.aspen.fuse.protocol.messages.MkdirRequest
import com.ibm.aspen.fuse.protocol.messages.UnlinkRequest
import com.ibm.aspen.fuse.protocol.messages.ForgetRequest

object WireProtocol {
  
  // The kernel limits all data transfers to 32 pages. We'll add an extra one just in case the kernel decides to use
  // the full 32 pages for data and sends an extra one contianing the message header. 
  // TODO: This is hard-coded to assume 4Kb pages. Fix it.
  val ReadBufferSizeInKb = 132
  
  //
  // Reply Header Format:
  //    <4-byte length (including header, reply, and data)>, <error_code>, <unique>
  //
  private def replyError(channel: GatheringByteChannel, unique: Long, error: Int): Unit = {
    val bb = ByteBuffer.allocate(16)
    bb.order(ByteOrder.nativeOrder())
    
    bb.putInt(16)
    bb.putInt(-error) // Note the negation of the error argument
    bb.putLong(unique)
    
    bb.flip()
    println(s"Responding with error code $error")
    channel.write(bb)
  }
  
  private def replyOk(channel: GatheringByteChannel, protocolVersion: ProtocolVersion, unique: Long, reply: Reply): Unit = {
    val staticReplyLength = reply.staticReplyLength
    
    val bb = ByteBuffer.allocate(16 + staticReplyLength)
    bb.order(ByteOrder.nativeOrder())
    val intent = 16 + staticReplyLength + reply.dataLength
    bb.putInt(16 + staticReplyLength + reply.dataLength)
    bb.putInt(0)
    bb.putLong(unique)
    reply.write(bb) // call this even if the staticReplyLength is zero. Provides a handle for message finalization
    
    bb.flip()
    
    if (reply.dataBuffers.isEmpty) {
      if (bb.remaining() != intent)
        println(s"***************** Sending WRONG NUMBER OF BYTES ${bb.remaining()} should be sending $intent")
      channel.write(bb)
    } else {
      channel.write((bb :: reply.dataBuffers).toArray)
    }
      
    reply.releaseBuffers()
  }
  
  def apply(
      mountPoint: String, 
      subtype: String, 
      mountFlags: Long, 
      fuseMountOptions: Option[String], 
      ops: FuseOptions,
      bufferManager: IOBufferManager,
      read_channel: ScatteringByteChannel,
      write_channel: GatheringByteChannel ): WireProtocol = {
    
    @tailrec
    def handshake(obb: Option[ByteBuffer]): WireProtocol = {
      println("*** Beginning Handshake ***")
      
      // Minimum Fuse read buffer is 8Kb
      val bb = obb match { 
        case Some(b) => 
          b.clear()
          b
        case None => bufferManager.allocateDirectBuffer(8)
      }

      read_channel.read(bb)
      
      bb.flip()
      
      val header = new RequestHeader(bb)
      
      println(s"Handshake header: $header")
      
      if (header.opcode != FUSE_INIT) {
        println("NOT A FUSE INIT MESSAGE")
        replyError(write_channel, header.unique, LinuxAPI.EIO)
        handshake(Some(bb))
      } 
      else {
        val ini = InitRequest(ProtocolVersion(0,0), header, bb)
        
        println(s"Handshake init: $ini")
        
        val kernelProtocolVersion = ProtocolVersion(ini.major, ini.minor)
        //println(s"Got init request: $ini")
        
        // Anything below major version 7 is too far back
        if (kernelProtocolVersion < ProtocolVersion(7,0)) 
          throw new UnsupportedFuseProtocolVersion(ini.major, ini.minor)
        
        else if (ini.major > CurrentProtocolVersion.major) {
          // Kernel version is too high. Request a lower protocol version
          val reply = new InitReply(kernelProtocolVersion, 
                                    CurrentProtocolVersion.major, CurrentProtocolVersion.minor, 0, 0, 0, 0, 0, 0)
          
          replyOk(write_channel, ProtocolVersion(0,0), header.unique, reply)
          
          // Wait for next request to arrive
          handshake(Some(bb))
        } 
        else {

          // Always enable. This is superseded by the max_write argument
          val FUSE_BIG_WRITES = 1 << 5
          
          val capabilities = (ini.flags & ops.requestedCapabilities) | FUSE_BIG_WRITES
          
          // We can set max_readahead smaller than what the kernel supports but we can't make it larger
          val max_readahead = if (ini.max_readahead < ops.max_readahead) ini.max_readahead else ops.max_readahead
          
          val max_write = ops.max_write
          
          val max_background = if (ops.max_background > 0xFFFF) 0xFFFF else ops.max_background
            
          val congestion_threshold = if (ops.congestion_threshold == 0) 
            (max_background * 3.0/4.0).asInstanceOf[Int] 
          else {
            if (ops.congestion_threshold > 0xFFFF) 0xFFFF else ops.congestion_threshold
          }
          
          val time_gran = if (kernelProtocolVersion >= ProtocolVersion(7,23)) ops.time_gran else 1
          
          val reply = InitReply(
              kernelProtocolVersion,
              CurrentProtocolVersion.major, 
              CurrentProtocolVersion.minor,
              max_readahead,
              capabilities,
              max_background.asInstanceOf[Short],
              congestion_threshold.asInstanceOf[Short],
              max_write,
              time_gran)
            
          replyOk(write_channel, kernelProtocolVersion, header.unique, reply)
          
          val actualOps = FuseOptions(max_readahead, max_background, congestion_threshold, max_write, time_gran, capabilities)
          
          bufferManager.returnBuffer(bb)
          
          new WireProtocol(mountPoint, subtype, read_channel, write_channel, kernelProtocolVersion, actualOps, bufferManager)
        }
      }
    }
    
    handshake(None)
  }
}

class WireProtocol private (
    val mountPoint: String, 
    val subtype: String,
    private val read_channel: ScatteringByteChannel,
    private val write_channel: GatheringByteChannel,
    val protocolVersion: ProtocolVersion,
    val options: FuseOptions,
    val bufferManager: IOBufferManager) {
  
  import WireProtocol._
  
  def unmount(): Unit = {
    read_channel.close()
    write_channel.close()
    LinuxAPI.fuse_umount(mountPoint)
  }
  
  def replyError(unique: Long, error: Int): Unit = WireProtocol.replyError(write_channel, unique, error)
  
  def replyOk(unique: Long, reply: Reply): Unit = WireProtocol.replyOk(write_channel, protocolVersion, unique, reply)
  
  private[this] var rbuff: ByteBuffer = bufferManager.allocateDirectBuffer(ReadBufferSizeInKb)
  private[this] var altBuff: ByteBuffer = bufferManager.allocateDirectBuffer(ReadBufferSizeInKb)
  private[this] val nullBuffer = ByteBuffer.allocate(0).asReadOnlyBuffer()
  
  rbuff.order(ByteOrder.nativeOrder())
  altBuff.order(ByteOrder.nativeOrder())
  rbuff.limit(0)
  altBuff.limit(0)
  
  /** Reads and returns a buffer of the requested size */
  def read(nbytes: Int): ByteBuffer = synchronized {
    // rbuff.position is always at the position of the next message to be read
    if (nbytes == 0) 
      nullBuffer
    else {
      println(s"Reading $nbytes. Pos ${rbuff.position} Remaining: ${rbuff.remaining()}")
      
      val (startPos, endPos) = if (rbuff.remaining() >= nbytes) {
        val startPos = rbuff.position()
        val endPos = rbuff.position + nbytes
        rbuff.position(endPos)
        (startPos, endPos)
      } else {
        
        if (rbuff.position + nbytes > rbuff.capacity()) {
          altBuff.position(0)
          altBuff.limit(altBuff.capacity())
          altBuff.put(rbuff)
          val t = rbuff
          rbuff = altBuff
          altBuff = t
        }
        
        val startPos = rbuff.position()
        val endPos = rbuff.position + nbytes
        
        rbuff.limit(endPos)
  
        while (rbuff.position < endPos) { 
          if (read_channel.read(rbuff) < 0)
            throw new LostConnection
        }
        
        rbuff.limit(rbuff.position)
        rbuff.position(endPos)
        
        (startPos, endPos)
      }
      
      val msg = rbuff.asReadOnlyBuffer()
      msg.position(startPos)
      msg.limit(endPos)
      msg.order(ByteOrder.nativeOrder())
      msg
    }
  }
  
  
    
  /** Reads and returns the next valid request.
   *  
   *  TODO - Enable ref-counted sharing of read Data buffers
   *     - Manager allocates a RefcountedReadBuffer which maintains a refcount and pointer to buffer manager
   *     - When requests require a data buffer, They incref the RefcountedReadBuffer and create a ReadOnlyByteBuffer w/
   *       their slice of the buffer
   *     - When response is sent, the request is released() 
   *     - users may want to be able to incref themselves to persist the data buffer beyond the normal release
   *  
   *  Throws LostConnection if the fuse connection to the kernel is lost
   *  @return the next supported Request
   */
  @tailrec
  final def readNextRequest(): Request = synchronized {
    println("**** WAITING FOR REQUEST ****")
    
    val hbb = read(RequestHeader.HeaderSize)
    println(s"Read header size: ${hbb.remaining}. Position ${hbb.position} Lim ${hbb.limit} Cap ${hbb.capacity}")
    val header = new RequestHeader(hbb)
    println(s"Received Request: $header")
    println(s"Reading data size: ${header.len - RequestHeader.HeaderSize}")
    val data   = read(header.len - RequestHeader.HeaderSize)
    println(s"Data Size: ${data.remaining}")
    
    
    
    header.opcode match {
      
      case OpCode.FUSE_OPENDIR    => OpenDirRequest(protocolVersion, header, data)
      case OpCode.FUSE_GETATTR    => GetAttrRequest(protocolVersion, header, data)
      case OpCode.FUSE_READDIR    => ReadDirRequest(protocolVersion, header, data)
      case OpCode.FUSE_RELEASE    => ReleaseRequest(protocolVersion, header, data)
      case OpCode.FUSE_RELEASEDIR => ReleaseDirRequest(protocolVersion, header, data)
      case OpCode.FUSE_LOOKUP     => LookupRequest(protocolVersion, header, data)
      case OpCode.FUSE_OPEN       => OpenRequest(protocolVersion, header, data)
      case OpCode.FUSE_READ       => ReadRequest(protocolVersion, header, data)
      case OpCode.FUSE_WRITE      => WriteRequest(protocolVersion, header, data)
      case OpCode.FUSE_SETATTR    => SetAttrRequest(protocolVersion, header, data)
      case OpCode.FUSE_MKNOD      => MknodRequest(protocolVersion, header, data)
      case OpCode.FUSE_RENAME     => RenameRequest(protocolVersion, header, data)
      case OpCode.FUSE_MKDIR      => MkdirRequest(protocolVersion, header, data)
      case OpCode.FUSE_UNLINK     => UnlinkRequest(protocolVersion, header, data)
      case OpCode.FUSE_FORGET     => ForgetRequest(protocolVersion, header, data)
      
      case _ => 
        replyError(header.unique, LinuxAPI.ENOSYS)
        readNextRequest()
    }
  }
}
