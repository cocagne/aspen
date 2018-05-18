package com.ibm.aspen.cumulofs.impl

import scala.concurrent._
import scala.concurrent.duration._
import org.scalatest._
import com.ibm.aspen.base.TestSystemSuite
import com.ibm.aspen.base.impl.Bootstrap
import com.ibm.aspen.cumulofs.FileSystem
import org.scalactic.source.Position.apply
import com.ibm.aspen.cumulofs.DirectoryPointer
import com.ibm.aspen.base.task.LocalTaskGroup
import com.ibm.aspen.base.task.TaskGroupPointer
import java.util.UUID
import com.ibm.aspen.core.read.InvalidObject
import com.ibm.aspen.cumulofs.FileInode
import com.ibm.aspen.core.DataBuffer
import com.ibm.aspen.core.objects.DataObjectPointer
import com.ibm.aspen.cumulofs.error.HolesNotSupported

class SimpleFileWriteSuite extends TestSystemSuite with CumuloFSBootstrap {
  test("Write single byte to emtpy file") {
    for {
      fs <- bootstrap()
      rootDir <- fs.loadRoot()
      initialContent <- rootDir.getContents()
      newFilePointer <- rootDir.createFile("foo", mode=0, uid=1, gid=2)
      file <- fs.loadFile(newFilePointer)
      _ <- file.write(0, DataBuffer(Array[Byte](0)))
      file2 <- fs.loadFile(newFilePointer)
      sfile = file2.asInstanceOf[SimpleFile]
      sarr <- sfile.index.debugGetAllSegments()
      data <- sfile.debugRead()
    } yield {
      file.size should be (1)
      file2.size should be (1)
      sarr.length should be (1)
      data.length should be (1)
      data(0) should be (0)
    }
  }
  
  test("Write past end of file fails with HolesNotSupported (empty file)") {
    for {
      fs <- bootstrap()
      rootDir <- fs.loadRoot()
      initialContent <- rootDir.getContents()
      newFilePointer <- rootDir.createFile("foo", mode=0, uid=1, gid=2)
      file <- fs.loadFile(newFilePointer)
      err <- file.write(1, DataBuffer(Array[Byte](0))).failed
      file2 <- fs.loadFile(newFilePointer)
    } yield {
      file.size should be (0)
      file2.size should be (0)
      err.isInstanceOf[HolesNotSupported] should be (true)
    }
  }
  
  test("Write past end of file fails with HolesNotSupported (non-empty file)") {
    for {
      fs <- bootstrap()
      rootDir <- fs.loadRoot()
      initialContent <- rootDir.getContents()
      newFilePointer <- rootDir.createFile("foo", mode=0, uid=1, gid=2)
      file <- fs.loadFile(newFilePointer)
      _ <- file.write(0, DataBuffer(Array[Byte](0)))
      err <- file.write(2, DataBuffer(Array[Byte](0))).failed
      file2 <- fs.loadFile(newFilePointer)
      sfile = file2.asInstanceOf[SimpleFile]
      sarr <- sfile.index.debugGetAllSegments()
      data <- sfile.debugRead()
    } yield {
      file.size should be (1)
      file2.size should be (1)
      sarr.length should be (1)
      data.length should be (1)
      data(0) should be (0)
      err.isInstanceOf[HolesNotSupported] should be (true)
    }
  }
  
  test("Write to end of file") {
    for {
      fs <- bootstrap()
      rootDir <- fs.loadRoot()
      initialContent <- rootDir.getContents()
      newFilePointer <- rootDir.createFile("foo", mode=0, uid=1, gid=2)
      file <- fs.loadFile(newFilePointer)
      _ <- file.write(0, DataBuffer(Array[Byte](0)))
      _ <- file.write(1, DataBuffer(Array[Byte](1)))
      file2 <- fs.loadFile(newFilePointer)
      sfile = file2.asInstanceOf[SimpleFile]
      sarr <- sfile.index.debugGetAllSegments()
      data <- sfile.debugRead()
    } yield {
      file.size should be (2)
      file2.size should be (2)
      sarr.length should be (1)
      data.length should be (2)
      data(0) should be (0)
      data(1) should be (1)
    }
  }
  
  test("Multi-buffer write") {
    for {
      fs <- bootstrap()
      rootDir <- fs.loadRoot()
      initialContent <- rootDir.getContents()
      newFilePointer <- rootDir.createFile("foo", mode=0, uid=1, gid=2)
      file <- fs.loadFile(newFilePointer)
      _ <- file.write(0, List(DataBuffer(Array[Byte](0)), DataBuffer(Array[Byte](1))))
      file2 <- fs.loadFile(newFilePointer)
      sfile = file2.asInstanceOf[SimpleFile]
      sarr <- sfile.index.debugGetAllSegments()
      data <- sfile.debugRead()
    } yield {
      file.size should be (2)
      file2.size should be (2)
      sarr.length should be (1)
      data.length should be (2)
      data(0) should be (0)
      data(1) should be (1)
    }
  }
  
  test("Write splits buffer on empty file") {
    for {
      fs <- bootstrap(fileSegmentSize=3)
      rootDir <- fs.loadRoot()
      initialContent <- rootDir.getContents()
      newFilePointer <- rootDir.createFile("foo", mode=0, uid=1, gid=2)
      file <- fs.loadFile(newFilePointer)
      _ <- file.write(0, DataBuffer(Array[Byte](0, 1, 2, 3, 4, 5)))
      file2 <- fs.loadFile(newFilePointer)
      sfile = file2.asInstanceOf[SimpleFile]
      sarr <- sfile.index.debugGetAllSegments()
      data <- sfile.debugRead()
    } yield {
      file.size should be (6)
      file2.size should be (6)
      sarr.length should be (2)
      data.length should be (6)
      data(0) should be (0)
      data(1) should be (1)
      data(2) should be (2)
      data(3) should be (3)
      data(4) should be (4)
      data(5) should be (5)
    }
  }
  
  test("Write splits buffer on non-empty file") {
    for {
      fs <- bootstrap(fileSegmentSize=3)
      rootDir <- fs.loadRoot()
      initialContent <- rootDir.getContents()
      newFilePointer <- rootDir.createFile("foo", mode=0, uid=1, gid=2)
      file <- fs.loadFile(newFilePointer)
      _ <- file.write(0, DataBuffer(Array[Byte](0)))
      _ <- file.write(1, DataBuffer(Array[Byte](1, 2, 3, 4, 5)))
      file2 <- fs.loadFile(newFilePointer)
      sfile = file2.asInstanceOf[SimpleFile]
      sarr <- sfile.index.debugGetAllSegments()
      data <- sfile.debugRead()
    } yield {
      file.size should be (6)
      file2.size should be (6)
      sarr.length should be (2)
      data.length should be (6)
      data(0) should be (0)
      data(1) should be (1)
      data(2) should be (2)
      data(3) should be (3)
      data(4) should be (4)
      data(5) should be (5)
    }
  }
  
  test("Write multi-buffer split with partial overwrite") {
    for {
      fs <- bootstrap(fileSegmentSize=3)
      rootDir <- fs.loadRoot()
      initialContent <- rootDir.getContents()
      newFilePointer <- rootDir.createFile("foo", mode=0, uid=1, gid=2)
      file <- fs.loadFile(newFilePointer)
      _ <- file.write(0, DataBuffer(Array[Byte](0,9)))
      _ <- file.write(1, List(DataBuffer(Array[Byte](1, 2, 3)), DataBuffer(Array[Byte](4, 5))))
      file2 <- fs.loadFile(newFilePointer)
      sfile = file2.asInstanceOf[SimpleFile]
      sarr <- sfile.index.debugGetAllSegments()
      data <- sfile.debugRead()
    } yield {
      file.size should be (6)
      file2.size should be (6)
      sarr.length should be (2)
      data.length should be (6)
      data(0) should be (0)
      data(1) should be (1)
      data(2) should be (2)
      data(3) should be (3)
      data(4) should be (4)
      data(5) should be (5)
    }
  }
  
  test("Write on segment boundary") {
    for {
      fs <- bootstrap(fileSegmentSize=3)
      rootDir <- fs.loadRoot()
      initialContent <- rootDir.getContents()
      newFilePointer <- rootDir.createFile("foo", mode=0, uid=1, gid=2)
      file <- fs.loadFile(newFilePointer)
      _ <- file.write(0, DataBuffer(Array[Byte](0, 1, 2)))
      _ <- file.write(3, DataBuffer(Array[Byte](3, 4, 5)))
      file2 <- fs.loadFile(newFilePointer)
      sfile = file2.asInstanceOf[SimpleFile]
      sarr <- sfile.index.debugGetAllSegments()
      data <- sfile.debugRead()
    } yield {
      file.size should be (6)
      file2.size should be (6)
      sarr.length should be (2)
      data.length should be (6)
      data(0) should be (0)
      data(1) should be (1)
      data(2) should be (2)
      data(3) should be (3)
      data(4) should be (4)
      data(5) should be (5)
    }
  }
  
  test("Write within single segment") {
    for {
      fs <- bootstrap(fileSegmentSize=3)
      rootDir <- fs.loadRoot()
      initialContent <- rootDir.getContents()
      newFilePointer <- rootDir.createFile("foo", mode=0, uid=1, gid=2)
      file <- fs.loadFile(newFilePointer)
      _ <- file.write(0, DataBuffer(Array[Byte](0, 1, 2, 3, 4, 5)))
      _ <- file.write(1, DataBuffer(Array[Byte](9)))
      file2 <- fs.loadFile(newFilePointer)
      sfile = file2.asInstanceOf[SimpleFile]
      sarr <- sfile.index.debugGetAllSegments()
      data <- sfile.debugRead()
    } yield {
      file.size should be (6)
      file2.size should be (6)
      sarr.length should be (2)
      data.length should be (6)
      data(0) should be (0)
      data(1) should be (9)
      data(2) should be (2)
      data(3) should be (3)
      data(4) should be (4)
      data(5) should be (5)
    }
  }
  
  test("Write within multiple segments") {
    for {
      fs <- bootstrap(fileSegmentSize=3)
      rootDir <- fs.loadRoot()
      initialContent <- rootDir.getContents()
      newFilePointer <- rootDir.createFile("foo", mode=0, uid=1, gid=2)
      file <- fs.loadFile(newFilePointer)
      _ <- file.write(0, DataBuffer(Array[Byte](0, 1, 2, 3, 4, 5)))
      _ <- file.write(1, DataBuffer(Array[Byte](9,10,11)))
      file2 <- fs.loadFile(newFilePointer)
      sfile = file2.asInstanceOf[SimpleFile]
      sarr <- sfile.index.debugGetAllSegments()
      data <- sfile.debugRead()
    } yield {
      file.size should be (6)
      file2.size should be (6)
      sarr.length should be (2)
      data.length should be (6)
      data(0) should be (0)
      data(1) should be (9)
      data(2) should be (10)
      data(3) should be (11)
      data(4) should be (4)
      data(5) should be (5)
    }
  }
}