package com.ibm.aspen.cumulofs.impl

import com.ibm.aspen.cumulofs.FileLoader
import com.ibm.aspen.cumulofs.FileSystem
import com.ibm.aspen.cumulofs.FilePointer
import scala.concurrent.ExecutionContext
import scala.concurrent.Future
import com.ibm.aspen.cumulofs.File

class SimpleFileLoader extends FileLoader {
  def loadFile(fs: FileSystem, pointer: FilePointer)(implicit ec: ExecutionContext): Future[File] = {
    val falloc = fs.defaultSegmentAllocater()
    val finode = fs.inodeLoader.load(pointer)
    for {
      allocater <- falloc
      inode <- finode 
    } yield {
     new SimpleFile(fs, inode) 
    }
  }
}