package com.ibm.aspen.fuse

import com.ibm.aspen.fuse.protocol.Capabilities._
import com.ibm.aspen.fuse.protocol.IOBufferManager
import com.ibm.aspen.fuse.protocol.messages._
import java.nio.ByteBuffer
import java.nio.channels.ScatteringByteChannel
import java.nio.channels.GatheringByteChannel

object Test {
  
  def main(args: Array[String]) {
    println("Trying to mount")
    val mountPoint = "/tmnt"
    println(s"Trying to mount sfuse at $mountPoint")
    
      /*case class FuseOptions(
    val	max_readahead:          Int,
    	val	max_background:         Short,
    	val	congestion_threshold:   Short,
    	val	max_write:              Int,
    	val	time_gran:              Int,
    	val	requestedCapabilities:  Int)*/
    
    val caps = FUSE_CAP_ASYNC_READ
    
    val ops = FuseOptions(
      max_readahead = 10,
      max_background = 10,
      congestion_threshold = 7,
      max_write = 16*1024,
      time_gran = 1,
      requestedCapabilities = caps)
    
    //val channel = LinuxAPI.fuse_mount(mountPoint, "sfuse", 0, None)
    val channel = RemoteFuse.connect("127.0.0.1", 1111, None)
    val fs = new TestFS(mountPoint, "sfuse", 0, None, ops, channel, channel)
    println("************* Initialized **************")
    fs.startHandlerDaemonThread()
    Thread.sleep(15000)
    println("Shutting down")
    fs.unmount()
  }
  
  class TestFS(
    mountPoint: String, 
    fsType: String, 
    mountFlags: Long, 
    fuseMountOptions: Option[String],
    requestedOps: FuseOptions,
    read_channel: ScatteringByteChannel,
    write_channel: GatheringByteChannel,
    obufferManager: Option[IOBufferManager] = None
    ) extends LowLevelFuseFilesystem(mountPoint, fsType, mountFlags, fuseMountOptions, requestedOps, 
                                     read_channel, write_channel, obufferManager) {
    
    val fstr = "These are the times that try men's souls\n"
    val fbytes = fstr.getBytes("UTF-8")
    
    val stats = Map(
        (1L -> Stat( inode   = 1, 
                  size       = 1, 
                  blocks     = 1, 
                  atime      = 0,
                  mtime      = 0,
                  ctime      = 0,
                  atimensec  = 0,
                  mtimensec  = 0,
                  ctimensec  = 0,
                  mode       = LinuxAPI.S_IFDIR | 0x1ed, // 0x1ed is 0755 (scala doesn't support octal cause it's "obsolete". Dumb.)
                  nlink      = 2,
                  uid        = 0,
                  gid        = 0,
                  rdev       = 0,
                  blksize    = 0)),
        (2L -> Stat( inode   = 2, 
                  size       = 1, 
                  blocks     = 1, 
                  atime      = 0,
                  mtime      = 0,
                  ctime      = 0,
                  atimensec  = 0,
                  mtimensec  = 0,
                  ctimensec  = 0,
                  mode       = LinuxAPI.S_IFDIR | 0x1ed, // 0x1ed is 0755
                  nlink      = 2,
                  uid        = 0,
                  gid        = 0,
                  rdev       = 0,
                  blksize    = 0)),
        (3L -> Stat( inode   = 3, 
                  size       = fbytes.length, 
                  blocks     = 1, 
                  atime      = 0,
                  mtime      = 0,
                  ctime      = 0,
                  atimensec  = 0,
                  mtimensec  = 0,
                  ctimensec  = 0,
                  mode       = LinuxAPI.S_IFREG | 0x124, // 0x1ed is 0444
                  nlink      = 2,
                  uid        = 0,
                  gid        = 0,
                  rdev       = 0,
                  blksize    = 0)))
    
    override def getattr(request: GetAttrRequest, response: Response[GetAttrReply]): Unit = {
      stats.get(request.inode) match {
        case Some(stat) => response.ok(new GetAttrReply(1, 0, stat))
        case None => response.error(LinuxAPI.ENOENT)
      }
    }
    
    override def lookup(request: LookupRequest, response: Response[DirEntryReply]): Unit = {
      println(s"Lookup request for ${request.name}")
      stats.get(3L) match {
        case Some(stat) => 
          val d = DirEntry( inode = 3,
                            generation = 0,
                            stat = stat,
                            attrTimeout = 10,
                            attrTimeoutNsec = 0,
                            entryTimeout = 10,
                            entryTimeoutNsec = 0)
          response.ok(new DirEntryReply(d))
        case None => response.error(LinuxAPI.ENOENT)
      }
    }
    
    override def open(request: OpenRequest, response: Response[OpenReply]): Unit = {
      if (request.inode == 3L) {
        println(s"FS: $request")
        println("Returning file handle 10")
        response.ok(new OpenReply(10, request.flags))
      } else
        response.error(LinuxAPI.EINVAL) 
    }
    
    override def read(request: ReadRequest, response: Response[DataReply]): Unit = {
      if (request.inode == 3L) {
        println(s"FS: $request")
        println(s"fbytes.length: ${fbytes.length}")
        response.ok(DataReply(ByteBuffer.wrap(fbytes)))
      } else
        response.error(LinuxAPI.EINVAL) 
    } 
    
    override def opendir(request: OpenDirRequest, response: Response[OpenReply]): Unit = {
      val r = new OpenReply(fileHandle=request.inode, openFlags=request.flags)
      response.ok(r)
      //response.error(LinuxAPI.ENOSYS)
    }
    
    var rdcount = 0
    override def readdir(request: ReadDirRequest, response: Response[ReadDirReply]): Unit = {
      println(s"Reading Directory!")
      
      val r = new ReadDirReply()
      
      if (rdcount == 0) {
        rdcount += 1
        r.appendEntry(1, 1, ".", FileType.Directory)
        r.appendEntry(2, 2, "..", FileType.Directory)
        r.appendEntry(3, 3, "hello_world", FileType.RegularFile)
      }
      
      response.ok(r)
    }
  }
}
