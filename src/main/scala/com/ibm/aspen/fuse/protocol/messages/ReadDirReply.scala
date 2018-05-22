package com.ibm.aspen.fuse.protocol.messages

import com.ibm.aspen.fuse.protocol.IOBufferManager
import com.ibm.aspen.fuse.protocol.Reply
import com.ibm.aspen.fuse.protocol.ProtocolVersion
import java.nio.ByteBuffer
import com.ibm.aspen.fuse.FileType
import com.ibm.aspen.fuse.LinuxAPI
import java.nio.charset.StandardCharsets
import com.ibm.aspen.fuse.FuseOptions

/*
struct fuse_dirent {
	uint64_t	ino;
	uint64_t	off;
	uint32_t	namelen;
	uint32_t	type;
	char name[];
};
*/
object ReadDirReply {
  val BaseDirEntrySize = 8*2 + 4*2
}

/** Note, adding directory entries to this class is NOT thread safe
 * 
 * The message format for ReadDir replies is a series of 8-byte aligned fuse_dirent structures 
 * terminated with a zeroed entry. 
 */
class ReadDirReply(
    implicit 
    val protocolVersion: ProtocolVersion, 
    val fuseOptions: FuseOptions,
    val bufferManager: IOBufferManager) extends Reply {
  
  import ReadDirReply._
  
  // Start off with a 16 KB byte buffer. This should be sufficient for most reasonably-sized directories
  private var buffers: List[ByteBuffer] = bufferManager.allocateDirectBuffer(16) :: Nil
  private var bytesWritten: Int = 0 
  
  def staticReplyLength: Int = 0 // Data-only response
  
  override def dataLength: Int = bytesWritten
  
  def write(bb:ByteBuffer): Unit = {
    // Flip buffers so they're ready for writing out to the file stream
    buffers.foreach( _.flip )
  }
  
  override def dataBuffers: List[ByteBuffer] = buffers.reverse
  
  override def releaseBuffers(): Unit = bufferManager.returnBuffers(buffers)
  
  /** Appends a directory entry to the data buffer.
   *  
   *  @offset A non-zero value that the filesystem can use to identify the position of this
   *          entry within the directory stream. Zero is reserved to mean "from the beginning"
   *          and therefore may not be used (this is checked via an assertion).
   *          
   *  @inode Inode number
   *  
   *  @name Textual name for the directory entry
   *  
   *  @fileType Enumerated file type
   *
   *  @return True if the entry was successfully appended. False if the maximum buffer size was reached and the
   *          prevented the entry from being added
   */
  def appendEntry(offset: Long, inode: Long, name: String, fileType: FileType.Value): Boolean = {
    require(offset > 0)
    
    val itype = fileType match {
      case FileType.UnixSocket      => LinuxAPI.S_IFSOCK >> 12
      case FileType.SymbolicLink    => LinuxAPI.S_IFLNK  >> 12
      case FileType.RegularFile     => LinuxAPI.S_IFREG  >> 12
      case FileType.BlockDevice     => LinuxAPI.S_IFBLK  >> 12
      case FileType.Directory       => LinuxAPI.S_IFDIR  >> 12
      case FileType.CharacterDevice => LinuxAPI.S_IFCHR  >> 12
      case FileType.Fifo            => LinuxAPI.S_IFFIFO >> 12
    }
    
    val nameBytes = name.getBytes(StandardCharsets.UTF_8)
    println(s"Sending name bytes ${com.ibm.aspen.util.arr2string(nameBytes)} len ${nameBytes.length}")
    val rawEntryLength = 2*8 + 2*4 + nameBytes.length
    val fullEntryLength = rawEntryLength + getPaddingToAlignment(rawEntryLength, 8)
    
    val currentDataLength = buffers.foldLeft(0)((accum, bb) => accum + bb.position)
    
    if (fullEntryLength + currentDataLength > fuseOptions.max_write)
      return false // Data Buffers are full
      
    if (fullEntryLength > buffers.head.remaining())
      buffers = bufferManager.allocateDirectBuffer(32) :: buffers
    
    buffers.head.putLong(inode)
    buffers.head.putLong(offset)
    buffers.head.putInt(nameBytes.length)
    buffers.head.putInt(itype)
    buffers.head.put(nameBytes)
    padToAlignment(buffers.head, 8)
    
    bytesWritten += fullEntryLength
    
    true
  }
}
