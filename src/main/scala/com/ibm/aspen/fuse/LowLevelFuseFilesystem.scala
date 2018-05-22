package com.ibm.aspen.fuse

import java.nio.channels.FileChannel
import java.nio.file.Paths
import java.nio.file.StandardOpenOption._

import com.ibm.aspen.fuse.protocol.WireProtocol
import com.ibm.aspen.fuse.protocol.messages.ReadDirRequest
import com.ibm.aspen.fuse.protocol.IOBufferManager
import com.ibm.aspen.fuse.protocol.messages.ReadDirReply
import com.ibm.aspen.fuse.protocol.messages.GetAttrRequest
import com.ibm.aspen.fuse.protocol.messages.GetAttrReply
import com.ibm.aspen.fuse.protocol.messages.OpenDirRequest
import com.ibm.aspen.fuse.protocol.messages.OpenReply
import com.ibm.aspen.fuse.protocol.messages.ReleaseRequest
import com.ibm.aspen.fuse.protocol.messages.ReleaseDirRequest
import com.ibm.aspen.fuse.protocol.messages.LookupRequest
import com.ibm.aspen.fuse.protocol.messages.DirEntryReply
import com.ibm.aspen.fuse.protocol.messages.OpenRequest
import com.ibm.aspen.fuse.protocol.messages.DataReply
import com.ibm.aspen.fuse.protocol.messages.ReadRequest
import java.nio.channels.ScatteringByteChannel
import java.nio.channels.GatheringByteChannel

 /** 
   *  
   *  @param mountPoint path to mount the filesystem at
   *  
   *  @param fsType Name of the filesystem type as displayed by mtab. This can be set to any short string.
   *  
   *  @param mountFlags zero or more of the MS_* flags combined with OR
   *  
   *  @param fuseMountOptions An optional string containing comma-separated arguments to pass through to the fuse
   *                          kernel module. Use "man mount.fuse" to view the available options.
   *                          
   *  @param obufferManager Replies often need to create IO buffers to store their resulting content. The kernel
   *                        limits the total number of outstanding requests that exist at any given point so we
   *                        can avoid allocation overhead by maintaining a pool of already-allocated buffers. If
   *                        no option is provided, a default pooling manager will be used
   *  
   */
class LowLevelFuseFilesystem(
    val mountPoint: String, 
    val fsType: String, 
    val mountFlags: Long, 
    val fuseMountOptions: Option[String],
    val requestedOps: FuseOptions,
    read_channel: ScatteringByteChannel,
    write_channel: GatheringByteChannel,
    obufferManager: Option[IOBufferManager] = None
    ) {
  
  implicit val bufferManager = obufferManager.getOrElse(new DefaultBufferManager)
  
  private val wp = WireProtocol(mountPoint, fsType, mountFlags, fuseMountOptions, requestedOps, bufferManager, read_channel, write_channel)
  
  implicit val protocolVersion = wp.protocolVersion
  implicit val supportedOps = wp.options
  
  def startHandlerDaemonThread() = {
    val dt = new Thread(new Runnable { override def run(): Unit = { reactorLoop() } }, "fuseHandlerDaemon");
    dt.setDaemon(true);
    dt.start();
  }
  
  def reactorLoop(): Unit = while(true) handleNextRequest()
  
  def handleNextRequest(): Unit = {
    wp.readNextRequest() match {
      case r: OpenDirRequest => opendir(r, new Response[OpenReply](wp, r.unique))
      case r: GetAttrRequest => getattr(r, new Response[GetAttrReply](wp, r.unique))
      case r: ReadDirRequest => readdir(r, new Response[ReadDirReply](wp, r.unique))
      case r: ReleaseRequest => release(r)
      case r: ReleaseDirRequest => releasedir(r)
      case r: LookupRequest => lookup(r, new Response[DirEntryReply](wp, r.unique))
      case r: OpenRequest => open(r, new Response[OpenReply](wp, r.unique))
      case r: ReadRequest => read(r, new Response[DataReply](wp, r.unique))
      case r =>
        println(s"UNSUPPORTED REQUEST TYPE $r")
    }
  }
  
  def unmount(): Unit = wp.unmount()
  
  def opendir(request: OpenDirRequest, response: Response[OpenReply]): Unit = response.error(LinuxAPI.ENOSYS) 
  def getattr(request: GetAttrRequest, response: Response[GetAttrReply]): Unit = response.error(LinuxAPI.ENOSYS)
  def readdir(request: ReadDirRequest, response: Response[ReadDirReply]): Unit = response.error(LinuxAPI.ENOSYS)
  def release(request: ReleaseRequest): Unit = {}
  def releasedir(request: ReleaseDirRequest): Unit = {}
  def lookup(request: LookupRequest, response: Response[DirEntryReply]): Unit = response.error(LinuxAPI.ENOSYS)
  def open(request: OpenRequest, response: Response[OpenReply]): Unit = response.error(LinuxAPI.ENOSYS)
  def read(request: ReadRequest, response: Response[DataReply]): Unit = response.error(LinuxAPI.ENOSYS)
}
