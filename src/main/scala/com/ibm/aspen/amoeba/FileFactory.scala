package com.ibm.aspen.amoeba

import com.ibm.aspen.core.objects.ObjectRevision

import scala.concurrent.{ExecutionContext, Future}



trait FileFactory {

  def createFileHandle(fs: FileSystem, file: File): FileHandle

  def createDirectory(fs: FileSystem,
                      pointer: DirectoryPointer,
                      inode: DirectoryInode,
                      revision: ObjectRevision)(implicit ec: ExecutionContext): Future[Directory]

  def createFile(fs: FileSystem,
                 pointer: FilePointer,
                 inode: FileInode,
                 revision: ObjectRevision)(implicit ec: ExecutionContext): Future[File]

  def createSymlink(fs: FileSystem,
                    pointer: SymlinkPointer,
                    inode: SymlinkInode,
                    revision: ObjectRevision)(implicit ec: ExecutionContext): Future[Symlink]

  def createUnixSocket(fs: FileSystem,
                       pointer: UnixSocketPointer,
                       inode: UnixSocketInode,
                       revision: ObjectRevision)(implicit ec: ExecutionContext): Future[UnixSocket]

  def createFIFO(fs: FileSystem,
                 pointer: FIFOPointer,
                 inode: FIFOInode,
                 revision: ObjectRevision)(implicit ec: ExecutionContext): Future[FIFO]

  def createCharacterDevice(fs: FileSystem,
                            pointer: CharacterDevicePointer,
                            inode: CharacterDeviceInode,
                            revision: ObjectRevision)(implicit ec: ExecutionContext): Future[CharacterDevice]

  def createBlockDevice(fs: FileSystem,
                        pointer: BlockDevicePointer,
                        inode: BlockDeviceInode,
                        revision: ObjectRevision)(implicit ec: ExecutionContext): Future[BlockDevice]

}
