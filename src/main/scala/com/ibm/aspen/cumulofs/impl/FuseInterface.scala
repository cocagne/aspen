package com.ibm.aspen.cumulofs.impl

import com.ibm.aspen.fuse.FuseOptions
import java.nio.channels.ScatteringByteChannel
import java.nio.channels.GatheringByteChannel
import com.ibm.aspen.fuse.protocol.IOBufferManager
import com.ibm.aspen.fuse.LowLevelFuseFilesystem
import com.ibm.aspen.cumulofs.FileSystem
import com.ibm.aspen.fuse.protocol.messages.GetAttrRequest
import com.ibm.aspen.fuse.Response
import com.ibm.aspen.fuse.protocol.messages.GetAttrReply
import scala.concurrent.ExecutionContext
import scala.util.Failure
import scala.util.Success
import com.ibm.aspen.fuse.Stat
import com.ibm.aspen.cumulofs.File
import com.ibm.aspen.cumulofs.Symlink
import com.ibm.aspen.fuse.LinuxAPI
import com.ibm.aspen.fuse.protocol.messages.LookupRequest
import com.ibm.aspen.fuse.protocol.messages.DirEntryReply
import com.ibm.aspen.cumulofs.Directory
import com.ibm.aspen.fuse.DirEntry
import com.ibm.aspen.cumulofs.BaseFile
import com.ibm.aspen.fuse.protocol.messages.OpenRequest
import com.ibm.aspen.fuse.protocol.messages.OpenReply
import com.ibm.aspen.fuse.protocol.messages.ReadRequest
import com.ibm.aspen.fuse.protocol.messages.DataReply
import java.nio.ByteBuffer
import com.ibm.aspen.fuse.protocol.messages.OpenDirRequest
import com.ibm.aspen.fuse.protocol.messages.ReadDirRequest
import com.ibm.aspen.fuse.protocol.messages.ReadDirReply
import com.ibm.aspen.fuse.{FileType => FuseFileType}
import com.ibm.aspen.cumulofs.{FileType => CumuloFileType}
import com.ibm.aspen.fuse.protocol.messages.WriteRequest
import com.ibm.aspen.fuse.protocol.messages.WriteReply
import com.ibm.aspen.core.DataBuffer
import com.ibm.aspen.fuse.protocol.messages.SetAttrRequest
import com.ibm.aspen.cumulofs.Timespec
import com.ibm.aspen.fuse.protocol.messages.MknodRequest
import com.ibm.aspen.cumulofs.FileMode
import com.ibm.aspen.fuse.protocol.messages.RenameRequest
import com.ibm.aspen.fuse.protocol.messages.ErrorOnly
import scala.concurrent.Future
import com.ibm.aspen.fuse.protocol.messages.MkdirRequest
import com.ibm.aspen.fuse.protocol.messages.UnlinkRequest
import com.ibm.aspen.fuse.protocol.messages.ForgetRequest
import java.util.UUID

object FuseInterface {
  def toFuseFileType(ft: CumuloFileType.Value): FuseFileType.Value = ft match {
    case CumuloFileType.File            =>  FuseFileType.RegularFile           
    case CumuloFileType.Directory       =>  FuseFileType.Directory      
    case CumuloFileType.Symlink         =>  FuseFileType.SymbolicLink       
    case CumuloFileType.UnixSocket      =>  FuseFileType.UnixSocket     
    case CumuloFileType.CharacterDevice =>  FuseFileType.CharacterDevice
    case CumuloFileType.BlockDevice     =>  FuseFileType.BlockDevice    
    case CumuloFileType.FIFO            =>  FuseFileType.Fifo          
  }
  
  def stat(file: BaseFile): Stat = {
    
    val size = file match {
       case f: File => f.size
       case f: Symlink => f.size
       case _ => 1L
     }
    
    Stat( inode      = file.pointer.number, 
          size       = size, 
          blocks     = 1, 
          atime      = file.atime.seconds,
          mtime      = file.mtime.seconds,
          ctime      = file.ctime.seconds,
          atimensec  = file.atime.nanoseconds,
          mtimensec  = file.mtime.nanoseconds,
          ctimensec  = file.ctime.nanoseconds,
          mode       = file.mode,
          nlink      = file.linkCount,
          uid        = file.uid,
          gid        = file.gid,
          rdev       = 0,
          blksize    = 0)
  }
}

class FuseInterface(
    val fs: FileSystem,
    mountPoint: String, 
    fsType: String, 
    mountFlags: Long, 
    fuseMountOptions: Option[String],
    requestedOps: FuseOptions,
    read_channel: ScatteringByteChannel,
    write_channel: GatheringByteChannel,
    obufferManager: Option[IOBufferManager] = None
    )(implicit ec: ExecutionContext) extends LowLevelFuseFilesystem(mountPoint, fsType, mountFlags, fuseMountOptions, requestedOps, 
                                     read_channel, write_channel, obufferManager) 
{
  import FuseInterface._
  
  var openFiles = Map[Long, File]()    // InodeNumber -> open file
  var fileHandles = Map[Long, File]()  // FileHandle -> open file
  var openDirs = Set[Long]()
  var nextfd = 1L
  
  override def getattr(request: GetAttrRequest, response: Response[GetAttrReply]): Unit = {
    println(s"getattr request for ${request.inode}")
    fs.lookup(request.inode) onComplete {
      case Failure(cause) => response.error(LinuxAPI.ENOENT)
      
      case Success(ofile) => ofile match {
        case None => response.error(LinuxAPI.ENOENT)
        
        case Some(file) => response.ok(new GetAttrReply(1, 0, stat(file)))
    }
   }
  }
   
  override def lookup(request: LookupRequest, response: Response[DirEntryReply]): Unit = {
    println(s"Lookup request for ${request.name} in directory ${request.inode}")
    fs.lookup(request.inode) onComplete {
      case Failure(cause) =>
        println(s"Lookup failure0 :( $cause")
        response.error(LinuxAPI.ENOENT)
      
      case Success(ofile) => ofile match {
        case None => response.error(LinuxAPI.ENOENT)
        
        case Some(file) => file match {
          case dir: Directory => dir.getEntry(request.name) onComplete {
            case Failure(cause) =>
              println(s"Lookup failure1 :( $cause")
              response.error(LinuxAPI.ENOENT)
            
            case Success(None) =>
              println("Got None response :(")
              response.error(LinuxAPI.ENOENT)
            
            case Success(Some(pointer)) => fs.lookup(pointer) onComplete { 
              case Failure(cause) =>
                println(s"Lookup failure2 :( $cause")
                response.error(LinuxAPI.ENOENT)
              
              case Success(file) =>
                val d = DirEntry( inode = pointer.number,
                                  generation = 0,
                                  stat = stat(file),
                                  attrTimeout = 10,
                                  attrTimeoutNsec = 0,
                                  entryTimeout = 10,
                                  entryTimeoutNsec = 0)
                 response.ok(new DirEntryReply(d))   
            }
          }
          
          case nondir => response.error(LinuxAPI.EINVAL)
        }
      }
    }
  }
  
  override def forget(request: ForgetRequest): Unit = {
    println(s"Got forget request! $request")
  }
  
  override def unlink(request: UnlinkRequest, response: Response[ErrorOnly]): Unit = {
    println(s"Unlink request for ${request.name} in directory ${request.inode}")
    
    fs.lookup(request.inode) onComplete {
      case Failure(cause) =>
        println(s"unlink failure0 :( $cause")
        response.error(LinuxAPI.ENOENT)
      
      case Success(ofile) => ofile match {
        case None => response.error(LinuxAPI.ENOENT)
        
        case Some(file) => 
          file match {
            case dir: Directory => 
              dir.getEntry(request.name) onComplete {
                case Failure(cause) =>
                  println(s"unlink failure1 :( $cause")
                  response.error(LinuxAPI.EIO)
                
                case Success(None) => response.error(LinuxAPI.ENOENT)
                
                case Success(Some(pointer)) =>
                  if (pointer.ftype == CumuloFileType.Directory)
                    response.error(LinuxAPI.EINVAL)
                  else {
                    dir.delete(request.name) onComplete {
                      case Failure(cause) =>
                        println(s"unlink failure2 :( $cause")
                        response.error(LinuxAPI.EIO)
                      case Success(_) => response.error(0)
                    }
                  }
              }
            
            case nondir => response.error(LinuxAPI.EINVAL)
          }
        }
    }
  }
  
  
    
  override def open(request: OpenRequest, response: Response[OpenReply]): Unit = synchronized {
    val fd = nextfd
    nextfd += 1
    
    fs.lookup(request.inode) onComplete {
      case Failure(cause) => response.error(LinuxAPI.ENOENT)
      case Success(None) =>
        println(s"** INODE NOT FOUND ${request.inode}")
        response.error(LinuxAPI.ENOENT)
      case Success(Some(file: File)) => synchronized {
        openFiles += (file.pointer.number -> file)  
        fileHandles += (fd -> file)
        response.ok(new OpenReply(fd, request.flags))
      }
      case Success(Some(other)) => 
        println(s"OTHER $other")
        response.error(LinuxAPI.EINVAL)
      
    }
  }
  
  override def read(request: ReadRequest, response: Response[DataReply]): Unit = synchronized {
    fileHandles.get(request.fileHandle) match {
      case None => response.error(LinuxAPI.EINVAL)
      case Some(file) => file.debugRead().onComplete { 
        case Failure(cause) => response.error(LinuxAPI.EIO)
        case Success(data) =>
          val bb = ByteBuffer.wrap(data)
          if (request.offset > bb.limit())
            response.error(LinuxAPI.EIO)
          else {
            bb.position(request.offset.asInstanceOf[Int])
            response.ok(DataReply(bb))
          }
      }
    }
  }
  
  override def write(request: WriteRequest, response: Response[WriteReply]): Unit = synchronized {
    fileHandles.get(request.fileHandle) match {
      case None => response.error(LinuxAPI.EINVAL)
      case Some(file) => file.write(request.offset, DataBuffer(request.data)).onComplete { 
        case Failure(cause) => response.error(LinuxAPI.EIO)
        case Success(data) => response.ok(new WriteReply(request.size))
      }
    }
  }
  
  override def setattr(request: SetAttrRequest, response: Response[GetAttrReply]): Unit = synchronized {
    println(s"** Setattr: $request")
    
    // Check to see if we have an open file first. Fetch directly otherwise
    val ffile =  openFiles.get(request.inode) match {
      case None => fs.lookup(request.inode)
      case Some(f) => Future.successful(Some(f))
    }
    
    ffile onComplete {
      case Failure(cause) => response.error(LinuxAPI.ENOENT)
      case Success(None) =>
        println(s"** Setattr INODE NOT FOUND ${request.inode}")
        response.error(LinuxAPI.ENOENT)
        
      case Success(Some(file)) => 
        
        val ct = request.ctime.getOrElse(file.ctime)
        val mt = request.mtime.getOrElse(file.mtime)
        val at = request.atime.getOrElse(file.atime)
        val mode = request.mode.getOrElse(file.mode)
        val uid = request.newUID.getOrElse(file.uid)
        val gid = request.newGID.getOrElse(file.gid)
        
        file.setattr(uid, gid, ct, mt, at, mode) onComplete {
          case Failure(cause) => response.error(LinuxAPI.EIO)
          case Success(_) => response.ok(new GetAttrReply(1, 0, stat(file)))
        }
    }
  }
  
  override def mknod(request: MknodRequest, response: Response[DirEntryReply]): Unit = {
    fs.lookup(request.inode) onComplete {
      case Failure(cause) =>
        println(s"mknod failure $cause")
        response.error(LinuxAPI.ENOENT)
      
      case Success(Some(dir: Directory)) =>
        import FileMode._
        
        val r = (request.mode & S_IFMT) match {
          case S_IFSOCK => Some(dir.createUnixSocket(request.name, request.mode, request.uid, request.gid))
          case S_IFLNK  => Some(dir.createSymlink(request.name, request.mode, request.uid, request.gid, ""))
          case S_IFREG  => Some(dir.createFile(request.name, request.mode, request.uid, request.gid))
          case S_IFBLK  => Some(dir.createBlockDevice(request.name, request.mode, request.uid, request.gid, request.rdev))
          case S_IFDIR  => Some(dir.createDirectory(request.name, request.mode, request.uid, request.gid))
          case S_IFCHR  => Some(dir.createCharacterDevice(request.name, request.mode, request.uid, request.gid, request.rdev))
          case S_IFFIFO => Some(dir.createFIFO(request.name, request.mode, request.uid, request.gid))
          case _        => None
        }
        
        r match {
          case None => response.error(LinuxAPI.EINVAL)
          
          case Some(f) => 
            f.flatMap(pointer => fs.lookup(pointer)) onComplete {
              case Failure(cause) => response.error(LinuxAPI.EIO)
              
              case Success(file) => 
                
                val d = DirEntry( inode = file.pointer.number,
                                  generation = 0,
                                  stat = stat(file),
                                  attrTimeout = 10,
                                  attrTimeoutNsec = 0,
                                  entryTimeout = 10,
                                  entryTimeoutNsec = 0)
                                  
                println(s"Created file ${request.name} with inode ${file.pointer.number} and type ${file.getClass.getTypeName}")
                response.ok(new DirEntryReply(d)) 
            }
        }
      
      case Success(f) =>
        response.error(LinuxAPI.EINVAL)
    }
  }
  
  override def mkdir(request: MkdirRequest, response: Response[DirEntryReply]): Unit = {
    fs.lookup(request.inode) onComplete {
      case Failure(cause) =>
        println(s"mkdir failure $cause")
        response.error(LinuxAPI.ENOENT)
      
      case Success(Some(dir: Directory)) =>
        import FileMode._
        
        val f = dir.createDirectory(request.name, request.mode, request.uid, request.gid)
         
        f.flatMap(pointer => fs.lookup(pointer)) onComplete {
          case Failure(cause) => response.error(LinuxAPI.EIO)
          
          case Success(file) => 
            
            val d = DirEntry( inode = file.pointer.number,
                              generation = 0,
                              stat = stat(file),
                              attrTimeout = 10,
                              attrTimeoutNsec = 0,
                              entryTimeout = 10,
                              entryTimeoutNsec = 0)
            println(s"Created directory ${request.name} with inode ${file.pointer.number}")        
            response.ok(new DirEntryReply(d)) 
        }
      
      case Success(f) =>
        println(s"mkdir failure2 $f")
        response.error(LinuxAPI.EINVAL)
    }
  }
  
  override def rename(request: RenameRequest, response: Response[ErrorOnly]): Unit = {
    val foldDir = fs.lookup(request.inode)
    val fnewDir = fs.lookup(request.newDirInode)
    
    implicit val tx = fs.system.newTransaction()
    
    def prep(oldf: Option[BaseFile], newf: Option[BaseFile]): Future[Unit] = (oldf, newf) match {
      case (Some(oldDir: Directory), Some(newDir: Directory)) => 
        if (oldDir.pointer.pointer.uuid == newDir.pointer.pointer.uuid) {
          oldDir.prepareRename(request.oldName, request.newName)
        } else {
          oldDir.getEntry(request.oldName) flatMap { oiptr => 
            Future.sequence(List(
              oldDir.prepareDelete(request.oldName, decref=false),
              newDir.prepareInsert(request.newName, oiptr.get, incref=false)
            )).map(_=>())  
          }
        }
      case _ => throw new Exception("Bad inode pointers")
    }
    
    val f = for {
      oldDir <- foldDir
      newDir <- fnewDir
      _ <- prep(oldDir, newDir)
      _ <- tx.commit()
    } yield ()
    
    f onComplete {
      case Failure(cause) => 
        println(s"Failed rename operation: $cause")
        tx.invalidateTransaction(cause)
        response.error(LinuxAPI.EINVAL)
      case Success(_) => response.error(0)
    }
  }
  
  override def opendir(request: OpenDirRequest, response: Response[OpenReply]): Unit = {
    fs.lookup(request.inode) onComplete {
      case Failure(cause) =>
        println(s"opendir failure $cause")
        response.error(LinuxAPI.ENOENT)
      
      case Success(Some(dir: Directory)) => synchronized {
        println(s"Opendir okay! $dir")
        
        val fd = nextfd
        nextfd += 1
        
        openDirs += fd
        
        response.ok(new OpenReply(fileHandle=fd, openFlags=request.flags))
      }
      case Success(f) =>
        println(s"opendir result $f")
        response.error(LinuxAPI.EINVAL)
    }
  }
  
  
  override def readdir(request: ReadDirRequest, response: Response[ReadDirReply]): Unit = synchronized {
    println(s"Reading Directory!")
    if (openDirs.contains(request.fileHandle)) {
      openDirs -= request.fileHandle
      
      fs.lookup(request.inode) onComplete {
        case Failure(cause) => response.error(LinuxAPI.ENOENT)
        
        case Success(Some(dir: Directory)) => dir.getContents() onComplete {
          case Failure(cause) => response.error(LinuxAPI.EIO)
        
          case Success(contents) =>
            val r = new ReadDirReply()
        
            r.appendEntry(1, 1, ".", FuseFileType.Directory)
            r.appendEntry(2, 2, "..", FuseFileType.Directory)
            
            contents.foldLeft(3) { (offset, e) =>
              println(s"name ${e.name} inode ${e.pointer.number}")
              r.appendEntry(offset, e.pointer.number, e.name, toFuseFileType(e.pointer.ftype))
              offset + 1
            }
            
            response.ok(r)
        }
          
        case Success(_) => response.error(LinuxAPI.EINVAL)
      }
    } else {
      response.ok(new ReadDirReply())
    }
  }
}
