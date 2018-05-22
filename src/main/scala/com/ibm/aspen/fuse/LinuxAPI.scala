package com.ibm.aspen.fuse

import com.sun.jna.Native
import com.sun.jna.Library
import java.nio.file.Paths
import java.nio.file.Files
import java.nio.file.LinkOption
import java.nio.channels.FileChannel
import java.nio.file.StandardOpenOption.{READ, WRITE}

object LinuxAPI {
  
  // errno.h
  val EACCES    = 13 // Permission denied (POSIX.1)
  val EAGAIN    = 11 // Resource temporarily unavailable (may be the same value as EWOULDBLOCK) (POSIX.1)
  val EEXIST    = 17 // File exists (POSIX.1)
  val EIO       =  5 // Input/output error (POSIX.1)
  val EINTR     =  4 // Interrupted function call (POSIX.1); see signal(7).
  val EINVAL    = 22 // Invalid argument (POSIX.1)
  val EISDIR    = 21 // Is a directory (POSIX.1)
  val ELOOP     = 40 // Too many levels of symbolic links (POSIX.1)
  val ENOENT    =  2 // No such file or directory (POSIX.1)
  val ENOMEM    = 12 // Not enough space (POSIX.1)
  val ENOSPC    = 28 // No space left on device (POSIX.1)
  val ENOSYS    = 38 // Function not implemented (POSIX.1)
  val ENOTDIR   = 20 // Not a directory (POSIX.1)
  val ENOTEMPTY = 39 // Directory not empty (POSIX.1)
  val ENOTSUP   = 95 // Operation not supported (POSIX.1)
  val EOVERFLOW = 75 // Value too large to be stored in data type (POSIX.1)
  val EPERM     =  1 // Operation not permitted (POSIX.1)
  val EPROTO    = 71 // Protocol error (POSIX.1)
  val EROFS     = 30 // Read-only filesystem (POSIX.1)
  
  // sys/stat.h - File type masks (see man 2 stat for details)
  val S_IFMT    = 0xf000 // bit mask for the file type bit field
  val S_IFSOCK  = 0xc000 // unix socket
  val S_IFLNK   = 0xa000 // symbolic link
  val S_IFREG   = 0x8000 // regular file
  val S_IFBLK   = 0x6000 // block device
  val S_IFDIR   = 0x4000 // directory
  val S_IFCHR   = 0x2000 // character device
  val S_IFFIFO  = 0x1000 // FIFO
  
  // mount flags
  val MS_RDONLY      = 0x001
  val MS_NOSUID      = 0x002
  val MS_NODEV       = 0x004
  val MS_NOEXEC      = 0x008
  val MS_SYNCHRONOUS = 0x010
  val MS_NODIRATIME  = 0x800
  
  // umount flags
  val MNT_DETACH     = 0x002
  
  trait CLibrary extends Library {
    def getuid(): Int
    def getgid(): Int
    def mount(source:String, target: String, filesystemtype: String, mountflags: Long, data: String): Int
    def umount2(target:String, flags:Int): Int
    def getpagesize(): Int
  }
  
  lazy val libc = Native.loadLibrary("c", classOf[CLibrary])
  
  /** Opens /dev/fuse and mounts the fuse file system on the requested mount point. The returned integer is the
   *  Unix file descriptor associated with the kernel <-> userspace communication channel. There is no built-in
   *  mechansim in the JVM to convert this into a Java-style file descriptor. However, the effect may be achieved
   *  by opening "/proc/fd/XXXX" in read/write mode.
   *  
   *  @param mountPoint path to mount the filesystem at
   *  @param subtype Name of the filesystem type as displayed by mtab. This can be set to any short string.
   *  @param mountFlags zero or more of the MS_* flags combined with OR
   *  @param fuseMountOptions An optional string containing comma-separated arguments to pass through to the fuse
   *                          kernel module. Use "man mount.fuse" to view the available options.
   *  
   */
  def fuse_mount(mountPoint: String, subtype: String, mountFlags: Long, fuseMountOptions: Option[String]): FileChannel = {
    val mp = Paths.get(mountPoint)
    val rootmode = if (Files.isRegularFile(mp, LinkOption.NOFOLLOW_LINKS)) 
      S_IFREG
    else if (Files.isDirectory(mp, LinkOption.NOFOLLOW_LINKS))
      S_IFDIR
    else 
      throw new Exception(s"Mount point $mountPoint is not a regular file or directory")
    
    val fusePath = Paths.get("/dev/fuse")
    
    if (!Files.exists(fusePath, LinkOption.NOFOLLOW_LINKS))
      throw new Exception("/dev/fuse does not exist")
    
    val channel = FileChannel.open(fusePath, READ, WRITE)
    
    // Okay, we need to pass the unix file descriptor for the open /dev/fuse to the fuse kernel module as
    // a mount option. But, of course, Java doesn't provide easy access to the integer file descriptor. So
    // we have to cheat a bit. Linux tracks all open file descriptors in /proc/PID/fd/ as symlinks to the
    // file they refer to. As a short cut, processes can use "/proc/self" to easily reference themselves. So
    // what we do here is scan all of the open file handles and find the one that's symlinked to /dev/fuse
    //
    val fd = (new java.io.File("/proc/self/fd")).listFiles().find { f =>
      val p = f.toPath()
      Files.isSymbolicLink(p) && Files.readSymbolicLink(p).toString == "/dev/fuse"
    } map { f => f.getName.toInt } match {
      case Some(fd) => fd
      
      case None =>
        channel.close()
        throw new Exception("Failed to find the unix file descriptor to the /dev/fuse channel")
    }
        
    val user_id = libc.getuid()
    val group_id = libc.getgid()
    
    // Prefix any user-supplied options with this, apparently required, set of undocumented arguments
    // TODO: Ensure unsigned values for user_id and group_id
    val fuseOptionsPrefix = f"fd=$fd%d,rootmode=$rootmode%o,user_id=$user_id%d,group_id=$group_id%d"
    
    val fullOptions = fuseMountOptions match {
      case None => fuseOptionsPrefix
      case Some(ops) => fuseOptionsPrefix + "," + ops
    }
    
    val res = libc.mount(subtype, mountPoint, "fuse."+subtype, mountFlags, fullOptions)
    
    if (res < 0) {
      channel.close()
      throw new Exception("Failed to mount the fuse file system")
    }
    
    channel
  }
  
  /** Tears down the fuse mount point and closes the low-level file handle */
  def fuse_umount(mountPoint:String): Unit = {
    libc.umount2(mountPoint, MNT_DETACH)
  }
}
