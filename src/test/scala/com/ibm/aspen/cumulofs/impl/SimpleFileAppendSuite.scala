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

class SimpleFileAppendSuite extends TestSystemSuite with CumuloFSBootstrap {
  
  test("Create and load empty file") {
    for {
      fs <- bootstrap()
      rootDir <- fs.loadRoot()
      initialContent <- rootDir.getContents()
      newFilePointer <- rootDir.createFile("foo", mode=0, uid=1, gid=2)
      file <- fs.loadFile(newFilePointer)
    } yield {
      file.size should be (0)
    }
  }
  
  test("Append single byte") {
    for {
      fs <- bootstrap()
      rootDir <- fs.loadRoot()
      initialContent <- rootDir.getContents()
      newFilePointer <- rootDir.createFile("foo", mode=0, uid=1, gid=2)
      file <- fs.loadFile(newFilePointer)
      db = DataBuffer(Array[Byte](0))
      _ <- file.append(db)
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
  
  test("Append multiple bytes") {
    for {
      fs <- bootstrap()
      rootDir <- fs.loadRoot()
      initialContent <- rootDir.getContents()
      newFilePointer <- rootDir.createFile("foo", mode=0, uid=1, gid=2)
      file <- fs.loadFile(newFilePointer)
      db = DataBuffer(Array[Byte](0,1,2))
      _ <- file.append(db)
      file2 <- fs.loadFile(newFilePointer)
      sfile = file2.asInstanceOf[SimpleFile]
      sarr <- sfile.index.debugGetAllSegments()
      data <- sfile.debugRead()
    } yield {
      file.size should be (3)
      file2.size should be (3)
      sarr.length should be (1)
      data.length should be (3)
      data(0) should be (0)
      data(1) should be (1)
      data(2) should be (2)
    }
  }
  
  test("Append two full segments of bytes") {
    for {
      fs <- bootstrap(fileSegmentSize=5)
      rootDir <- fs.loadRoot()
      initialContent <- rootDir.getContents()
      newFilePointer <- rootDir.createFile("foo", mode=0, uid=1, gid=2)
      file <- fs.loadFile(newFilePointer)
      db = DataBuffer(Array[Byte](0,1,2,3,4,5,6,7,8,9))
      _ <- file.append(db)
      file2 <- fs.loadFile(newFilePointer)
      sfile = file2.asInstanceOf[SimpleFile]
      sarr <- sfile.index.debugGetAllSegments()
      data <- sfile.debugRead()
      d0 <- sys.readObject(sarr(0).pointer)
    } yield {
      file.size should be (10)
      file2.size should be (10)
      sarr.length should be (2)
      data.length should be (10)
      data(0) should be (0)
      data(1) should be (1)
      data(2) should be (2)
      data(3) should be (3)
      data(4) should be (4)
      data(5) should be (5)
      data(6) should be (6)
      data(7) should be (7)
      data(8) should be (8)
      data(9) should be (9)
    }
  }
  
  test("Append to partially filled segment, no split") {
    for {
      fs <- bootstrap(fileSegmentSize=5)
      rootDir <- fs.loadRoot()
      initialContent <- rootDir.getContents()
      newFilePointer <- rootDir.createFile("foo", mode=0, uid=1, gid=2)
      file <- fs.loadFile(newFilePointer)
      db = DataBuffer(Array[Byte](0,1))
      _ <- file.append(db)
      db2 = DataBuffer(Array[Byte](2,3))
      _ <- file.append(db2)
      file2 <- fs.loadFile(newFilePointer)
      sfile = file2.asInstanceOf[SimpleFile]
      sarr <- sfile.index.debugGetAllSegments()
      data <- sfile.debugRead()
      d0 <- sys.readObject(sarr(0).pointer)
    } yield {
      file.size should be (4)
      file2.size should be (4)
      sarr.length should be (1)
      data.length should be (4)
      data(0) should be (0)
      data(1) should be (1)
      data(2) should be (2)
      data(3) should be (3)
    }
  }
  
  test("Append to partially filled segment, exactly fill segment") {
    for {
      fs <- bootstrap(fileSegmentSize=5)
      rootDir <- fs.loadRoot()
      initialContent <- rootDir.getContents()
      newFilePointer <- rootDir.createFile("foo", mode=0, uid=1, gid=2)
      file <- fs.loadFile(newFilePointer)
      db = DataBuffer(Array[Byte](0,1))
      _ <- file.append(db)
      db2 = DataBuffer(Array[Byte](2,3,4))
      _ <- file.append(db2)
      file2 <- fs.loadFile(newFilePointer)
      sfile = file2.asInstanceOf[SimpleFile]
      sarr <- sfile.index.debugGetAllSegments()
      data <- sfile.debugRead()
      d0 <- sys.readObject(sarr(0).pointer)
    } yield {
      file.size should be (5)
      file2.size should be (5)
      sarr.length should be (1)
      data.length should be (5)
      data(0) should be (0)
      data(1) should be (1)
      data(2) should be (2)
      data(3) should be (3)
      data(4) should be (4)
    }
  }
  
  test("Append to partially filled segment, with split") {
    for {
      fs <- bootstrap(fileSegmentSize=5)
      rootDir <- fs.loadRoot()
      initialContent <- rootDir.getContents()
      newFilePointer <- rootDir.createFile("foo", mode=0, uid=1, gid=2)
      file <- fs.loadFile(newFilePointer)
      db = DataBuffer(Array[Byte](0,1))
      _ <- file.append(db)
      db2 = DataBuffer(Array[Byte](2,3,4,5,6,7))
      _ <- file.append(db2)
      file2 <- fs.loadFile(newFilePointer)
      sfile = file2.asInstanceOf[SimpleFile]
      sarr <- sfile.index.debugGetAllSegments()
      data <- sfile.debugRead()
      d0 <- sys.readObject(sarr(0).pointer)
    } yield {
      file.size should be (8)
      file2.size should be (8)
      sarr.length should be (2)
      data.length should be (8)
      data(0) should be (0)
      data(1) should be (1)
      data(2) should be (2)
      data(3) should be (3)
      data(4) should be (4)
      data(5) should be (5)
      data(6) should be (6)
      data(7) should be (7)
    }
  }
}