package com.ibm.amoeba.fs.impl.simple

import com.ibm.amoeba.client.internal.allocation.SinglePoolObjectAllocator
import com.ibm.amoeba.common.objects.ObjectRevisionGuard
import com.ibm.amoeba.fs.{DirectoryInode, DirectoryPointer, FileInode, FileType}

class SimpleFileSystemTestSuite extends FilesSystemTestSuite {

  test("Load root directory pointer") {
    for {
      fs <- bootFS()
      (rootInode, _, _) <- fs.readInode(1)
    } yield {
      rootInode.fileType should be (FileType.Directory)
    }
  }

  test("Change file uid") {
    for {
      fs <- bootFS()
      (rootInode, rootPointer, rootRevision) <- fs.readInode(1)
      dir = new SimpleDirectory(rootPointer.asInstanceOf[DirectoryPointer],
        rootRevision, rootInode.asInstanceOf[DirectoryInode], fs)
      originalUid = dir.uid
      _ <- dir.setUID(2)
      newUid = dir.uid
    } yield {
      rootInode.fileType should be (FileType.Directory)
      originalUid should be (0)
      newUid should be (2)
    }
  }

  test("Change file gid") {
    for {
      fs <- bootFS()
      (rootInode, rootPointer, rootRevision) <- fs.readInode(1)
      dir = new SimpleDirectory(rootPointer.asInstanceOf[DirectoryPointer],
        rootRevision, rootInode.asInstanceOf[DirectoryInode], fs)
      originalGid = dir.gid
      _ <- dir.setGID(2)
      newGid = dir.gid
    } yield {
      rootInode.fileType should be (FileType.Directory)
      originalGid should be (0)
      newGid should be (2)
    }
  }

  test("Prepare hardlink") {
    for {
      fs <- bootFS()
      (rootInode, rootPointer, rootRevision) <- fs.readInode(1)
      dir = new SimpleDirectory(rootPointer.asInstanceOf[DirectoryPointer],
        rootRevision, rootInode.asInstanceOf[DirectoryInode], fs)
      nlinks1 = dir.links
      tx = client.newTransaction()
      _ = dir.prepareHardLink()(tx)
      _ <- tx.commit()
      nlinks2 = dir.links
    } yield {
      rootInode.fileType should be (FileType.Directory)
      nlinks1 should be (1)
      nlinks2 should be (2)
    }
  }

  test("Create File") {
    val initInode = FileInode.init(0, 0, 1)
    for {
      fs <- bootFS()
      (rootInode, rootPointer, rootRevision) <- fs.readInode(1)
      dir = new SimpleDirectory(rootPointer.asInstanceOf[DirectoryPointer],
        rootRevision, rootInode.asInstanceOf[DirectoryInode], fs)
      tx = client.newTransaction()
      f <- CreateFileTask.prepareTask(fs, dir.pointer, "foo", initInode)(tx)
      _ <- tx.commit()
      _ <- f
      ofile <- dir.getEntry("foo")
      (file, _, _) <- fs.readInode(ofile.get)
    } yield {
      rootInode.fileType should be (FileType.Directory)
      file.fileType should be (FileType.File)
    }
  }
}