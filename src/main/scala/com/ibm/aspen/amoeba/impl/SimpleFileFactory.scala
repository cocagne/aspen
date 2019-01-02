package com.ibm.aspen.amoeba.impl

import com.ibm.aspen.amoeba._
import com.ibm.aspen.core.objects.ObjectRevision

import scala.concurrent.{ExecutionContext, Future}

class SimpleFileFactory(val writeBufferSize: Int) extends FileFactory {

  def createFileHandle(fs: FileSystem, file: File): FileHandle = {
    new SimpleFileHandle(file, writeBufferSize)
  }

  def createDirectory(fs: FileSystem,
                      pointer: DirectoryPointer,
                      inode: DirectoryInode,
                      revision: ObjectRevision)(implicit ec: ExecutionContext): Future[Directory] = {
    Future.successful(new SimpleDirectory(pointer, revision, inode, fs))
  }

  def createFile(fs: FileSystem,
                 pointer: FilePointer,
                 inode: FileInode,
                 revision: ObjectRevision)(implicit ec: ExecutionContext): Future[File] = {
    Future.successful(new SimpleFile(pointer, revision, inode, fs))
  }

  def createSymlink(fs: FileSystem,
                    pointer: SymlinkPointer,
                    inode: SymlinkInode,
                    revision: ObjectRevision)(implicit ec: ExecutionContext): Future[Symlink] = {
    Future.successful(new SimpleSymlink(pointer, inode, revision, fs))
  }

  def createUnixSocket(fs: FileSystem,
                       pointer: UnixSocketPointer,
                       inode: UnixSocketInode,
                       revision: ObjectRevision)(implicit ec: ExecutionContext): Future[UnixSocket] = {
    Future.successful(new SimpleUnixSocket(pointer, inode, revision, fs))
  }

  def createFIFO(fs: FileSystem,
                 pointer: FIFOPointer,
                 inode: FIFOInode,
                 revision: ObjectRevision)(implicit ec: ExecutionContext): Future[FIFO] = {
    Future.successful(new SimpleFIFO(pointer, inode, revision, fs))
  }

  def createCharacterDevice(fs: FileSystem,
                            pointer: CharacterDevicePointer,
                            inode: CharacterDeviceInode,
                            revision: ObjectRevision)(implicit ec: ExecutionContext): Future[CharacterDevice] = {
    Future.successful(new SimpleCharacterDevice(pointer, inode, revision, fs))
  }

  def createBlockDevice(fs: FileSystem,
                        pointer: BlockDevicePointer,
                        inode: BlockDeviceInode,
                        revision: ObjectRevision)(implicit ec: ExecutionContext): Future[BlockDevice] = {
    Future.successful(new SimpleBlockDevice(pointer, inode, revision, fs))
  }


}
