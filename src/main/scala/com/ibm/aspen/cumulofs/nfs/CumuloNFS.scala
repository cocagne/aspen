package com.ibm.aspen.cumulofs.nfs

import java.io.IOException
import java.nio.ByteBuffer

import com.github.blemale.scaffeine.{Cache, Scaffeine}
import com.ibm.aspen.core.read.FatalReadError
import com.ibm.aspen.cumulofs
import com.ibm.aspen.cumulofs.{DirectoryEntry => _, Inode => _, _}
import javax.security.auth.Subject
import org.apache.logging.log4j.scala.Logging
import org.dcache.auth.Subjects
import org.dcache.nfs.status._
import org.dcache.nfs.v4.{NfsIdMapping, SimpleIdMap}
import org.dcache.nfs.v4.xdr.nfsace4
import org.dcache.nfs.vfs._

import scala.collection.JavaConverters.asJavaCollection
import scala.concurrent.{ExecutionContext, Future}

// mount -v -t nfs4 -o "vers=4.1" 192.168.56.1:/ /mnt

object CumuloNFS {
  import scala.language.implicitConversions

  implicit def inode2long(inode: Inode): Long = ByteBuffer.wrap(inode.getFileId).getLong

  implicit def long2inode(fd: Long): Inode = {
    val arr = new Array[Byte](8)
    ByteBuffer.wrap(arr).putLong(fd)
    Inode.forFile(arr)
  }
}

class CumuloNFS(val fs: FileSystem,
                implicit val ec: ExecutionContext,
                inodeCacheMax: Int = 1000) extends VirtualFileSystem with Logging {

  import CumuloNFS._

  private val NoAcl = new Array[nfsace4](0)
  private val IdMapper = new SimpleIdMap

  private[this] val inodes: Cache[Long, NFSBaseFile] = Scaffeine().maximumSize(inodeCacheMax).build[Long, NFSBaseFile]()

  def load(inode: Long, cache: Boolean = true): NFSBaseFile = inodes.getIfPresent(inode) match {
    case Some(f) => f
    case None => blockingCall {
      fs.lookup(inode) map {
        case Some(f) =>
          val nfsFile = f match {
            case x: BlockDevice => new NFSBlockDevice(x)
            case x: CharacterDevice => new NFSCharacterDevice(x)
            case x: Directory => new NFSDirectory(x)
            case x: File => new NFSFile(x)
            case x: Symlink => new NFSSymlink(x)
            case x: FIFO => new NFSFIFO(x)
            case x: UnixSocket => new NFSUnixSocket(x)
          }
          if (cache)
            inodes.put(inode, nfsFile)
          nfsFile
        case None => throw new NoEntException(s"no such inode $inode")
      } recover {
        case _: FatalReadError => throw new ServerFaultException("sumthin broke")
      }
    }
  }

  private def getDirectory(inode: Long): NFSDirectory = load(inode) match {
    case d: NFSDirectory => d
    case _ => throw new NotDirException(s"Inode $inode is not a directory")
  }

  private def getFile(inode: Long): NFSFile = load(inode) match {
    case f: NFSFile => f
    case _ => throw new NotSuppException(s"Operation supported only on regular file")
  }

  private def getInodePointer(inode: Long): InodePointer = blockingCall {
    fs.lookupInodePointer(inode) map {
      case Some(iptr) => iptr
      case _ => throw new NotSuppException(s"Operation supported only on regular file")
    }
  }

  /**
    * Check access to file system object.
    *
    * @param inode inode of the object to check.
    * @param mode  a mask of permission bits to check.
    * @return an allowed subset of permissions from the given mask.
    * @throws IOException meh
    */
  @throws[IOException]
  def access(inode: Inode, mode: Int): Int = mode // always allow

  /**
    * Create a new object in a given directory with a specific name.
    *
    * @param parent  directory where new object must be created.
    * @param type    the type of the object to be created.
    * @param name    name of the object.
    * @param subject the owner subject of a newly created object.
    * @param mode    initial permission mask.
    * @return the inode of the newly created object.
    * @throws IOException meh
    */
  @throws[IOException]
  def create(parent: Inode, `type`: Stat.Type, name: String, subject: Subject, mode: Int): Inode = {
    logger.info(s"Create ${`type`} $name in directory ${inode2long(parent)}")

    val dir = getDirectory(parent)

    // TODO: Move this check into Directory impl. This is racy and incorrect with concurrent writers
    dir.getEntry(name) match {
      case Some(_) => throw new ExistException(s"$name already exists")
      case None =>
    }

    val uid = Subjects.getUid(subject).toInt
    val gid = Subjects.getPrimaryGid(subject).toInt

    val iptr = `type` match {
      case Stat.Type.REGULAR => dir.createFile(name, mode, uid, gid)
      case Stat.Type.DIRECTORY => dir.createDirectory(name, mode, uid, gid)
      case Stat.Type.SYMLINK => dir.createSymlink(name, mode, uid, gid, "")
      case Stat.Type.CHAR => dir.createCharacterDevice(name, mode, uid, gid, 0)
      case Stat.Type.BLOCK => dir.createBlockDevice(name, mode, uid, gid, 0)
      case Stat.Type.FIFO => dir.createFIFO(name, mode, uid, gid)
      case Stat.Type.SOCK => dir.createUnixSocket(name, mode, uid, gid)
      case _ => throw new NotSuppException("Unknown FileType")
    }

    logger.info(s"Created ${iptr.ftype} with name $name and inode ${iptr.number}")

    iptr.number
  }

  /**
    * Get file system's usage information.
    *
    * @return file system's usage information.
    * @throws IOException meh
    */
  @throws[IOException]
  def getFsStat: FsStat = {
    logger.info("getFsStat")
    val totalSpace = 1024*1024*1024
    val totalFiles = 100
    val usedSpace  = 1024*1024
    val usedFiles  = 50
    new FsStat(totalSpace, totalFiles, usedSpace, usedFiles)
  }

  /**
    * Get inode of the root object for given file system.
    *
    * @return inode of the root object.
    * @throws IOException meh
    */
  @throws[IOException]
  def getRootInode: Inode = 1L

  /**
    * Get inode of the object with a given name in provided directory.
    *
    * @param parent parent directory's inode.
    * @param name   object name.
    * @return inode of the object.
    * @throws IOException meh
    */
  @throws[IOException]
  def lookup(parent: Inode, name: String): Inode = {
    logger.info(s"lookup $parent $name")
    getDirectory(parent).getEntry(name) match {
      case Some(iptr) => iptr.number
      case None => throw new NoEntException(s"No such file $name")
    }
  }

  /**
    * Create a hard-link to an existing file system object.
    *
    * @param parent  directory, where new object must be created.
    * @param link    an inode of existing file system object.
    * @param name    name of the new object.
    * @param subject the owner subject of a newly created object.
    * @return inode of the newly created object.
    * @throws IOException meh
    */
  @throws[IOException]
  def link(parent: Inode, link: Inode, name: String, subject: Subject): Inode = {
    logger.info(s"link $name")
    val pdir = getDirectory(parent)
    val target = getInodePointer(link)

    // TODO: Move this check into Directory impl. This is racy and incorrect with concurrent writers
    pdir.getEntry(name) match {
      case Some(_) => throw new ExistException(s"$name already exists")
      case None =>
    }

    pdir.insert(name, target)

    target.number
  }

  /**
    * Get list of file system objects in the given directory. The provided
    * cookie identifies a logical offset of the listing, if directory
    * listing is processed in chunks. The verifie argument used to
    * validate cookies as directory content can be changes and earlier generated
    * cookie cannot be used any more. For initial listing a zero cookie and verifier
    * is used. The returned listing will contain only entries with cookies
    * greater than specified value.
    *
    * @param inode    inode of the directory to list.
    * @param verifier opaque verifier to identify { @code snapshot} to list.
    * @param cookie a logical offset in the listing.
    * @return DirectoryStream containing directory listing.
    * @throws IOException meh
    */
  @throws[IOException]
  def list(inode: Inode, verifier: Array[Byte], cookie: Long): DirectoryStream = {
    logger.info(s"List directory ${inode2long(inode)}")

    val dir = getDirectory(inode)

    val entries = dir.getContents().zipWithIndex.map { t =>
      val (cumulofs.DirectoryEntry(name, iptr), cookie) = t
      val file = load(iptr.number, cache=false)
      new DirectoryEntry(name, iptr.number, file.nfsStat, cookie)
    }

    new DirectoryStream(DirectoryStream.ZERO_VERIFIER, asJavaCollection(entries))
  }

  /**
    * Generate a opaque directory verifier which is identified with can
    * be used as identifier of directory's state snapshot.
    *
    * @param inode inode of the directory to create verifier.
    * @return opaque verifier.
    * @throws IOException meh
    */
  @throws[IOException]
  def directoryVerifier(inode: Inode): Array[Byte] = DirectoryStream.ZERO_VERIFIER

  /**
    * Create a new sub-directory in a given directory.
    *
    * @param parent  directory, where new sub-directory must be created.
    * @param name    the name of the newly created sub-directory.
    * @param subject the owner subject of a newly created sub-directory.
    * @param mode    initial permission mask.
    * @return inode of the newly created sub-directory.
    * @throws IOException meh
    */
  @throws[IOException]
  def mkdir(parent: Inode, name: String, subject: Subject, mode: Int): Inode = synchronized {
    logger.info(s"mkdir in dir ${inode2long(parent)} name $name mode ${mode.toOctalString}")

    val pdir = getDirectory(parent)

    // TODO: Move this check into Directory impl. This is racy and incorrect with concurrent writers
    pdir.getEntry(name) match {
      case Some(_) => throw new ExistException(s"$name already exists")
      case None =>
    }

    val uid = Subjects.getUid(subject).toInt
    val gid = Subjects.getPrimaryGid(subject).toInt

    val iptr = pdir.createDirectory(name, mode, uid, gid)

    logger.info(s"Created new directory $name in parent dir ${pdir.pointer.number} with inode number ${iptr.number}")

    iptr.number
  }

  /**
    * Move file system object from one directory to another.
    *
    * @param src     the directory from which to move the object.
    * @param oldName object's name in the source directory.
    * @param dest    the directory where the file system object should be moved.
    * @param newName object's name in the new directory.
    * @return true if file system was changed.
    * @throws IOException meh
    */
  @throws[IOException]
  def move(src: Inode, oldName: String, dest: Inode, newName: String): Boolean = synchronized {
    val s = getDirectory(src)
    val d = getDirectory(dest)

    logger.info(s"move $oldName to $newName. Src ${s.pointer.number} Dst ${d.pointer.number}")

    // TODO: Move this check into Directory impl. This is racy and incorrect with concurrent writers
    d.getEntry(newName) match {
      case Some(_) => throw new ExistException(s"$newName already exists")
      case None =>
    }

    s.getEntry(oldName) match {
      case None => throw new NoEntException(s"$oldName does not exist")
      case Some(iptr) =>

        val odir = iptr.ftype match {
          case FileType.Directory => Some(getDirectory(iptr.number))
          case _ => None
        }

        val refreshOnFailure = s :: d :: odir.map(t => t :: Nil).getOrElse(Nil)

        retryTransactionUntilSuccessful(fs.system, refreshOnFailure) { implicit tx =>
          // TODO: Fix race conditions with concurrent writers. Need to accruately detect failures and error out
          s.file.prepareDelete(oldName, decref = false)
          d.file.prepareInsert(newName, iptr, incref = false)

          odir.foreach { tdir =>
            val updatedInode = tdir.file.inode.setParentDirectory(Some(d.file.pointer))

            tx.overwrite(tdir.file.pointer.pointer, tdir.file.revision, updatedInode.toDataBuffer)
          }

          Future.unit
        }

        true
    }
  }

  /**
    * Get parent directory of a given object.
    *
    * @param inode inode of a file system object for which the parent inode is
    *              desired.
    * @return parent directory of the given object.
    * @throws IOException meh
    */
  @throws[IOException]
  def parentOf(inode: Inode): Inode = getDirectory(inode).parentInodeNumber

  /**
    * Read data from file with a given inode into data.
    *
    * @param inode  inode of the file to read from.
    * @param data   byte array for writing.
    * @param offset file's position to read from.
    * @param count  number of bytes to read.
    * @return number of bytes read from the file, possibly zero. -1 if EOF is
    *         reached.
    * @throws IOException meh
    */
  @throws[IOException]
  def read(inode: Inode, data: Array[Byte], offset: Long, count: Int): Int = synchronized {
    val f = getFile(inode)
    logger.info(s"read ${f.inodeNumber} offset $offset count $count")
    if (offset > f.len)
      -1
    else {
      val maxread = if (count <= data.length) count else data.length
      val remaining = f.len - offset
      if (remaining == 0)
        0
      else {
        val nread = if (remaining < maxread) remaining else maxread
        ByteBuffer.wrap(f.data).get(data, offset.asInstanceOf[Int], nread.asInstanceOf[Int])
        nread.asInstanceOf[Int]
      }
    }
  }

  /**
    * Get value of a symbolic link object.
    *
    * @param inode symbolic link's inode.
    * @return value of a symbolic link.
    * @throws IOException meh
    */
  @throws[IOException]
  def readlink(inode: Inode): String = synchronized {
    getInode(inode) match {
      case l: MLink => l.link
      case _ => throw new NotSuppException("Operation requires symlink inode")
    }
  }

  /**
    * Remove a file system object from the given directory and possibly the
    * object itself.
    *
    * @param parent directory from which file system object is removed.
    * @param name   object's name in the directory.
    * @throws IOException meh
    */
  @throws[IOException]
  def remove(parent: Inode, name: String): Unit = synchronized {
    val dir = getDirectory(parent)
    dir.entries.find(t => t._1 == name) match {
      case None => throw new NoEntException(s"No such file $name")
      case Some(t) =>
        t._2 match {
          case sub: MDir => if (sub.entries.nonEmpty) throw new NotEmptyException(s"$name directory is not empty")
          case _ =>
        }

        dir.delete(name)

        if (t._2.stats.getNlink == 0) {
          usedFiles -= 1
          t._2 match {
            case f: MFile => usedSpace -= f.data.length
            case _ =>
          }
          inodes -= t._2.inodeNumber
        }
    }
  }

  /**
    * Create a symbolic link.
    *
    * @param parent  inode of the directory, where symbolic link is created.
    * @param name    name of the symbolic link object.
    * @param link    the value the symbolic link points to.
    * @param subject the owner subject of a newly created object.
    * @param mode    initial permission mask.
    * @return inode of newly created object.
    * @throws IOException meh
    */
  @throws[IOException]
  def symlink(parent: Inode, name: String, link: String, subject: Subject, mode: Int): Inode = synchronized {
    val pdir = getDirectory(parent)

    val lnk = new MLink(allocInode(), mode)

    lnk.link = link

    lnk.stats.setUid(Subjects.getUid(subject).toInt)
    lnk.stats.setGid(Subjects.getPrimaryGid(subject).toInt)

    pdir.add(name, lnk)
    addInode(lnk)

    lnk.inodeNumber
  }

  /**
    * Write provided data into inode with a given stability level.
    *
    * @param inode          inode of the file to write.
    * @param data           data to be written.
    * @param offset         the file position to begin writing at.
    * @param count          number of bytes to write.
    * @param stabilityLevel data stability level.
    * @return write result.
    * @throws IOException meh
    */
  @throws[IOException]
  def write(inode: Inode, data: Array[Byte], offset: Long, count: Int, stabilityLevel: VirtualFileSystem.StabilityLevel): VirtualFileSystem.WriteResult = synchronized {
    logger.info(s"write ${inode2long(inode)} offset $offset count $count")

    val f = getFile(inode)

    val wend = offset + count

    var arr = f.data

    if (wend > f.data.length) {
      var sz = f.data.length * 2
      while (sz < wend)
        sz *= 2
      arr = new Array[Byte](sz)
      ByteBuffer.wrap(arr).put(f.data)
    }

    ByteBuffer.wrap(arr).put(data, offset.asInstanceOf[Int], count)

    if (wend > f.len)
      f.len = wend.asInstanceOf[Int]

    f.data = arr

    new VirtualFileSystem.WriteResult(stabilityLevel, count)
  }

  /**
    * Flush data in dirty state to the stable storage. Typically
    * follows write() operation.
    *
    * @param inode  inode of the file to commit.
    * @param offset the file position to start commit at.
    * @param count  number of bytes to commit.
    * @throws IOException meh
    */
  @throws[IOException]
  def commit(inode: Inode, offset: Long, count: Int): Unit = ()

  /**
    * Get file system object's attributes.
    *
    * @param inode inode of the file system object.
    * @return file's attributes.
    * @throws IOException meh
    */
  @throws[IOException]
  def getattr(inode: Inode): Stat = synchronized { getInode(inode).stats }

  /**
    * Set/update file system object's attributes.
    *
    * @param inode inode of the file system object.
    * @param stat  file's attributes to set.
    * @throws IOException meh
    */
  @throws[IOException]
  def setattr(inode: Inode, stat: Stat): Unit = synchronized {
    val i = getInode(inode)

    if (stat.isDefined(Stat.StatAttribute.OWNER))
      i.stats.setUid(stat.getUid)

    if (stat.isDefined(Stat.StatAttribute.GROUP))
      i.stats.setGid(stat.getGid)

    if (stat.isDefined(Stat.StatAttribute.DEV)) i match {
      case _: MBlock => i.stats.setDev(stat.getDev)
      case _: MChar => i.stats.setDev(stat.getDev)
      case _ => new InvalException("Can't set device attribute on non-device file")
    }

    if (stat.isDefined(Stat.StatAttribute.RDEV)) i match {
      case _: MBlock => i.stats.setRdev(stat.getRdev)
      case _: MChar => i.stats.setRdev(stat.getRdev)
      case _ => new InvalException("Can't set rdevice attribute on non-device file")
    }


    if (stat.isDefined(Stat.StatAttribute.MODE))
      i.stats.setMode((stat.getMode & ~Stat.S_TYPE) | (i.stats.getMode | Stat.S_TYPE))

    if (stat.isDefined(Stat.StatAttribute.SIZE)) i match {
      case f: MFile =>
        val newSize = stat.getSize.asInstanceOf[Int]
        if (newSize > f.len) {
          val arr = new Array[Byte](newSize)
          ByteBuffer.wrap(arr).put(f.data)
          usedSpace += newSize - f.len
          f.len = newSize
          f.data = arr
        }
        else if (newSize < f.len) {
          val arr = new Array[Byte](newSize)
          ByteBuffer.wrap(f.data).get(arr)
          usedSpace -= f.len - newSize
          f.len = newSize
          f.data = arr
        }
      case _ => throw new InvalException("Can't change size of non-regular file")
    }

    if (stat.isDefined(Stat.StatAttribute.ATIME)) i.stats.setATime(stat.getATime)
    if (stat.isDefined(Stat.StatAttribute.MTIME)) i.stats.setMTime(stat.getMTime)
    if (stat.isDefined(Stat.StatAttribute.CTIME)) i.stats.setCTime(stat.getCTime)
  }

  /**
    * Get file system object's Access Control List.
    *
    * @param inode inode of the file system object.
    * @return object's access control list.
    * @throws IOException meh
    */
  @throws[IOException]
  def getAcl(inode: Inode): Array[nfsace4] = NoAcl

  /**
    * Set file system object's Access Control List.
    *
    * @param inode inode of the file system object.
    * @param acl   access control list to set.
    * @throws IOException meh
    */
  @throws[IOException]
  def setAcl(inode: Inode, acl: Array[nfsace4]): Unit = ()

  /**
    * Returns true if file system object eligible for pNFS operations.
    *
    * @param inode inode of the file system object to test for PNFS operations.
    * @return true if file system object eligible for pNFS operations.
    * @throws IOException meh
    */
  @throws[IOException]
  def hasIOLayout(inode: Inode): Boolean =  false

  /**
    * Get instance of a AclCheckable object which can perform access
    * control list check.
    *
    * @return instance of AclCheckable.
    */
  def getAclCheckable: AclCheckable = AclCheckable.UNDEFINED_ALL

  /**
    * Get instance of a NfsIdMapping object which can provide principal
    * identity mapping.
    *
    * @return instance of NfsIdMapping.
    */
  def getIdMapper: NfsIdMapping = IdMapper
}
