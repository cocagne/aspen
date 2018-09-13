package com.ibm.aspen.cumulofs.impl

import com.ibm.aspen.cumulofs.{File, FileLoader, FilePointer, FileSystem}

import scala.concurrent.{ExecutionContext, Future}

class SimpleFileLoader extends FileLoader {
  def loadFile(fs: FileSystem, pointer: FilePointer)(implicit ec: ExecutionContext): Future[File] = {
    val falloc = fs.defaultSegmentAllocater()
    val finode = fs.inodeLoader.load(pointer)
    for {
      allocater <- falloc
      (inode, revision) <- finode
    } yield {
     new SimpleFile(pointer, revision, inode, fs)
    }
  }
}