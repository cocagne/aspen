package com.ibm.aspen.fuse

import java.nio.channels.{GatheringByteChannel, ScatteringByteChannel}

import com.ibm.aspen.fuse.protocol.{IOBufferManager, ProtocolVersion, WireProtocol}
import com.ibm.aspen.fuse.protocol.messages._
import org.apache.logging.log4j.scala.Logging

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
    ) extends Logging {
  
  implicit val bufferManager: IOBufferManager = obufferManager.getOrElse(new DefaultBufferManager)
  
  private val wp = WireProtocol(mountPoint, fsType, mountFlags, fuseMountOptions, requestedOps, bufferManager, read_channel, write_channel)
  
  implicit val protocolVersion: ProtocolVersion = wp.protocolVersion
  implicit val supportedOps: FuseOptions = wp.options
  
  def startHandlerDaemonThread(): Unit = {
    val dt = new Thread( () => reactorLoop(), "fuseHandlerDaemon")
    dt.setDaemon(true)
    dt.start()
  }
  
  def reactorLoop(): Unit = while(true) handleNextRequest()
  
  def handleNextRequest(): Unit = {
    val req = wp.readNextRequest()
    logger.info(s"Received $req")
    req match {
      case r: OpenDirRequest => opendir(r, new Response[OpenReply](wp, r.unique))
      case r: GetAttrRequest => getattr(r, new Response[GetAttrReply](wp, r.unique))
      case r: ReadDirRequest => readdir(r, new Response[ReadDirReply](wp, r.unique))
      case r: ReleaseRequest => release(r)
      case r: ReleaseDirRequest => releasedir(r)
      case r: LookupRequest => lookup(r, new Response[DirEntryReply](wp, r.unique))
      case r: OpenRequest => open(r, new Response[OpenReply](wp, r.unique))
      case r: ReadRequest => read(r, new Response[DataReply](wp, r.unique))
      case r: WriteRequest => write(r, new Response[WriteReply](wp, r.unique))
      case r: SetAttrRequest => setattr(r, new Response[GetAttrReply](wp, r.unique))
      case r: MknodRequest => mknod(r, new Response[DirEntryReply](wp, r.unique))
      case r: MkdirRequest => mkdir(r, new Response[DirEntryReply](wp, r.unique))
      case r: RenameRequest => rename(r, new Response[ErrorOnly](wp, r.unique))
      case r: UnlinkRequest => unlink(r, new Response[ErrorOnly](wp, r.unique))
      case r: ForgetRequest => forget(r)
      case r =>
        logger.error(s"UNSUPPORTED REQUEST TYPE $r")
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
  def write(request: WriteRequest, response: Response[WriteReply]): Unit = response.error(LinuxAPI.ENOSYS)
  def setattr(request: SetAttrRequest, response: Response[GetAttrReply]): Unit = response.error(LinuxAPI.ENOSYS)
  def mknod(request: MknodRequest, response: Response[DirEntryReply]): Unit = response.error(LinuxAPI.ENOSYS)
  def mkdir(request: MkdirRequest, response: Response[DirEntryReply]): Unit = response.error(LinuxAPI.ENOSYS)
  def rename(request: RenameRequest, response: Response[ErrorOnly]): Unit = response.error(LinuxAPI.ENOSYS)
  def unlink(request: UnlinkRequest, response: Response[ErrorOnly]): Unit = response.error(LinuxAPI.ENOSYS)
  def forget(request: ForgetRequest): Unit = {}
}
