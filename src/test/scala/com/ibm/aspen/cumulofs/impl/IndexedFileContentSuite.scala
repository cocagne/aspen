package com.ibm.aspen.cumulofs.impl

import com.ibm.aspen.base.{TestSystemSuite, Transaction}
import com.ibm.aspen.core.DataBuffer

import scala.concurrent.Future



class IndexedFileContentSuite extends TestSystemSuite with CumuloFSBootstrap {
  
  def boot(osegmentSize: Option[Int]=None, otierNodeSize: Option[Int]=None): Future[SimpleFile] = for {
    fs <- bootstrap()

    rootDir <- fs.loadRoot()

    tx = fs.system.newTransaction()

    fdir <- rootDir.prepareCreateFile("foo", mode=0, uid=1, gid=2)(tx, executionContext)

    _=tx.commit()

    newFilePointer <- fdir

    (newInode, revision) <- fs.inodeLoader.load(newFilePointer)
  } yield {
    new SimpleFile(newFilePointer, revision, newInode, fs, osegmentSize, otierNodeSize)
  }
  
  test("Read empty File") {
    for {
      file <- boot()
      a <- file.debugReadFully()
    } yield {
      a.length should be (0)
    }
  }
  
  test("Read write empty file") {
    implicit val tx:Transaction = sys.newTransaction()
    val w = Array[Byte](5)
    for {
      file <- boot()

      (remainingOffset, remainingData) <- file.write(0, w)

      a <- file.debugReadFully()
    } yield {
      file.inode.size should be (w.length)
      remainingOffset should be (0)
      remainingData should be (Nil)
      a should be (w)
    }
  }
  
  test("Read write empty file with hole") {
    implicit val tx:Transaction = sys.newTransaction()
    val w = Array[Byte](5)
    val e = Array[Byte](0,0,5)
    for {
      file <- boot(Some(5), Some(500))
      (remainingOffset, remainingData) <- file.write(2, w)
      a <- file.debugReadFully()
    } yield {
      file.inode.size should be (3)
      remainingOffset should be (0)
      remainingData should be (Nil)
      a should be (e)
    }
  }
  
  test("Read write empty file with full segment hole") {
    implicit val tx:Transaction = sys.newTransaction()
    val w = Array[Byte](5)
    val e = Array[Byte](0,0,0,0,0,0,5)
    for {
      file <- boot(Some(5), Some(500))
      (remainingOffset, remainingData) <- file.write(6, w)
      a <- file.debugReadFully()
    } yield {
      file.inode.size should be (7)
      remainingOffset should be (0)
      remainingData should be (Nil)
      a should be (e)
    }
  }
  
  test("Partial read, single segment") {
    implicit val tx:Transaction = sys.newTransaction()
    val w = Array[Byte](1,2,3,4,5,6,7,8,9)
    for {
      file <- boot(Some(5), Some(500))
      (remainingOffset, remainingData) <- file.write(0, w)
      a <- file.read(2,2)
    } yield {
      file.inode.size should be (w.length)
      remainingOffset should be (0)
      remainingData should be (Nil)
      a.get.getByteArray() should be (Array[Byte](3,4))
    }
  }
  test("Partial read, multi segment") {
    implicit val tx:Transaction = sys.newTransaction()
    val w = Array[Byte](1,2,3,4,5,6,7,8,9)
    for {
      file <- boot(Some(5), Some(500))
      (remainingOffset, remainingData) <- file.write(0, w)
      a <- file.read(2,5)
    } yield {
      file.inode.size should be (w.length)
      remainingOffset should be (0)
      remainingData should be (Nil)
      a.get.getByteArray() should be (Array[Byte](3,4,5,6,7))
    }
  }
  
  test("Write two segments") {
    implicit val tx:Transaction = sys.newTransaction()
    val w = Array[Byte](1,2,3,4,5,6,7,8,9)
    for {
      file <- boot(Some(5), Some(500))
      (remainingOffset, remainingData) <- file.write(0, w)
      a <- file.debugReadFully()
    } yield {
      file.inode.size should be (w.length)
      remainingOffset should be (0)
      remainingData should be (Nil)
      a should be (w)
    }
  }
  
  test("Write three segments") {
    implicit val tx:Transaction = sys.newTransaction()
    val w = Array[Byte](1,2,3,4,5,6,7,8,9,10,11)
    for {
      file <- boot(Some(5), Some(500))
      (remainingOffset, remainingData) <- file.write(0, w)
      a <- file.debugReadFully()
    } yield {
      file.inode.size should be (w.length)
      remainingOffset should be (0)
      remainingData should be (Nil)
      a should be (w)
    }
  }
  
  test("Write emtpy to multi-tier index") {
    implicit val tx:Transaction = sys.newTransaction()
    val w = Array[Byte](1,2,3,4,5,6,7,8,9,10,11,13,14,15,16)
    for {
      file <- boot(Some(5), Some(500))
      (remainingOffset, remainingData) <- file.write(0, w)
      a <- file.debugReadFully()
    } yield {
      file.inode.size should be (w.length)
      remainingOffset should be (0)
      remainingData should be (Nil)
      a should be (w)
    }
  }
  
  test("Multi-buffer write") {
    implicit val tx:Transaction = sys.newTransaction()
    val a = Array[Byte](5,6)
    val b = Array[Byte](7,8)
    val w = Array[Byte](5,6,7,8)
    for {
      file <- boot(Some(5), Some(500))
      (remainingOffset, remainingData) <- file.write(0, List(DataBuffer(a),DataBuffer(b)))
      a <- file.debugReadFully()
    } yield {
      file.inode.size should be (w.length)
      remainingOffset should be (0)
      remainingData should be (Nil)
      a should be (w)
    }
  }
  
  test("Overwrite single segment, no extend") {
    implicit val tx:Transaction = sys.newTransaction()
    val w1 = Array[Byte](0,1,2,3)
    val w2 = Array[Byte](4,5,6,7)
    for {
      file <- boot(Some(5), Some(500))
      (_, _) <- file.write(0, w1)
      a <- file.debugReadFully()
      (remainingOffset, remainingData) <- file.write(0, w2)
      b <- file.debugReadFully()
    } yield {
      file.inode.size should be (w1.length)
      remainingOffset should be (0)
      remainingData should be (Nil)
      a should be (w1)
      b should be (w2)
    }
  }
  
  test("Overwrite single segment, with extend") {
    implicit val tx:Transaction = sys.newTransaction()
    val w1 = Array[Byte](0,1)
    val w2 = Array[Byte](4,5,6,7)
    for {
      file <- boot(Some(5), Some(500))
      (_, _) <- file.write(0, w1)
      a <- file.debugReadFully()
      inode2 = file.inode
      (remainingOffset, remainingData) <- file.write(0, w2)
      b <- file.debugReadFully()
    } yield {
      inode2.size should be (w1.length)
      file.inode.size should be (w2.length)
      remainingOffset should be (0)
      remainingData should be (Nil)
      a should be (w1)
      b should be (w2)
    }
  }
  test("Overwrite segment with allocation leaves remaining data") {
    implicit val tx:Transaction = sys.newTransaction()
    val w1 = Array[Byte](0,1)
    val w2 = Array[Byte](0,1,2,3,4,5,6)
    val e = Array[Byte](0,1,2,3,4)
    for {
      file <- boot(Some(5), Some(500))
      (_, _) <- file.write(0, w1)
      a <- file.debugReadFully()
      (remainingOffset, remainingData) <- file.write(0, w2)
      b <- file.debugReadFully()
    } yield {
      file.inode.size should be (5)
      remainingOffset should be (5)
      remainingData.foldLeft(0)((sz, db) => sz + db.size) should be (2)
      a should be (w1)
      b should be (e)
    }
  }
  test("Allocate into hole, short segment") {
    implicit val tx:Transaction = sys.newTransaction()
    val w1 = Array[Byte](5)
    val w2 = Array[Byte](0,1,2,3)
    val e1 = Array[Byte](0,0,0,0,0,5)
    val e2 = Array[Byte](0,1,2,3,0,5)
    for {
      file <- boot(Some(5), Some(500))
      (_, _) <- file.write(5, w1)
      a <- file.debugReadFully()
      (remainingOffset, remainingData) <- file.write(0, w2)
      b <- file.debugReadFully()
    } yield {
      file.inode.size should be (6)
      remainingOffset should be (0)
      remainingData should be (Nil)
      a should be (e1)
      b should be (e2)
    }
  }
  test("Allocate into hole, full segment") {
    implicit val tx:Transaction = sys.newTransaction()
    val w1 = Array[Byte](5)
    val w2 = Array[Byte](0,1,2,3,4)
    val e1 = Array[Byte](0,0,0,0,0,5)
    val e2 = Array[Byte](0,1,2,3,4,5)
    for {
      file <- boot(Some(5), Some(500))
      (_, _) <- file.write(5, w1)
      a <- file.debugReadFully()
      (remainingOffset, remainingData) <- file.write(0, w2)
      b <- file.debugReadFully()
    } yield {
      file.inode.size should be (6)
      remainingOffset should be (0)
      remainingData should be (Nil)
      a should be (e1)
      b should be (e2)
    }
  }
  test("Allocate into hole, multi-segment") {
    implicit val tx:Transaction = sys.newTransaction()
    val w1 = Array[Byte](10)
    val w2 = Array[Byte](0,1,2,3,4,5,6,7,8,9)
    val e1 = Array[Byte](0,0,0,0,0,0,0,0,0,0,10)
    val e2 = Array[Byte](0,1,2,3,4,5,6,7,8,9,10)
    for {
      file <- boot(Some(5), Some(500))
      (_, _) <- file.write(10, w1)
      a <- file.debugReadFully()
      inode2 = file.inode
      (remainingOffset, remainingData) <- file.write(0, w2)
      b <- file.debugReadFully()
    } yield {
      inode2.size should be (e2.length)
      file.inode.size should be (e2.length)
      remainingOffset should be (0)
      remainingData should be (Nil)
      a should be (e1)
      b should be (e2)
    }
  }
  test("Allocate into hole, partial overwrite") {
    implicit val tx:Transaction = sys.newTransaction()
    val w1 = Array[Byte](5,6,7)
    val w2 = Array[Byte](0,1,2,3,4,9,9)
    val e1 = Array[Byte](0,0,0,0,0,5,6,7)
    val e2 = Array[Byte](0,1,2,3,4,9,9,7)
    for {
      file <- boot(Some(5), Some(500))
      (_, _) <- file.write(5, w1)
      a <- file.debugReadFully()
      inode2 = file.inode
      (remainingOffset, remainingData) <- file.write(0, w2)
      b <- file.debugReadFully()
    } yield {
      inode2.size should be (e1.length)
      file.inode.size should be (e2.length)
      remainingOffset should be (0)
      remainingData should be (Nil)
      a should be (e1)
      b should be (e2)
    }
  }
  test("Allocate into hole, full overwrite") {
    implicit val tx:Transaction = sys.newTransaction()
    val w1 = Array[Byte](5,6,7)
    val w2 = Array[Byte](0,1,2,3,4,9,9,9)
    val e1 = Array[Byte](0,0,0,0,0,5,6,7)
    val e2 = Array[Byte](0,1,2,3,4,9,9,9)
    for {
      file <- boot(Some(5), Some(500))
      (_, _) <- file.write(5, w1)
      a <- file.debugReadFully()
      inode2 = file.inode
      (remainingOffset, remainingData) <- file.write(0, w2)
      b <- file.debugReadFully()
    } yield {
      inode2.size should be (e1.length)
      file.inode.size should be (e2.length)
      remainingOffset should be (0)
      remainingData should be (Nil)
      a should be (e1)
      b should be (e2)
    }
  }
  test("Allocate into hole, multi-segment partial overwrite") {
    implicit val tx:Transaction = sys.newTransaction()
    val w1 = Array[Byte](5,6,7,8,9,10,11)
    val w2 = Array[Byte](0,1,2,3,4,15,16,17,18,19,20)
    val e1 = Array[Byte](0,0,0,0,0,5,6,7,8,9,10,11)
    val e2 = Array[Byte](0,1,2,3,4,15,16,17,18,19,20,11)
    for {
      file <- boot(Some(5), Some(500))
      (_, _) <- file.write(5, w1)
      a <- file.debugReadFully()
      inode2 = file.inode
      (remainingOffset, remainingData) <- file.write(0, w2)
      b <- file.debugReadFully()
    } yield {
      inode2.size should be (e1.length)
      file.inode.size should be (e2.length)
      remainingOffset should be (0)
      remainingData should be (Nil)
      a should be (e1)
      b should be (e2)
    }
  }
  test("Allocate into hole, multi-segment full overwrite") {
    implicit val tx:Transaction = sys.newTransaction()
    val w1 = Array[Byte](5,6,7,8,9,10,11)
    val w2 = Array[Byte](0,1,2,3,4,15,16,17,18,19,20,21)
    val e1 = Array[Byte](0,0,0,0,0,5,6,7,8,9,10,11)
    val e2 = Array[Byte](0,1,2,3,4,15,16,17,18,19,20,21)
    for {
      file <- boot(Some(5), Some(500))
      (_, _) <- file.write(5, w1)
      a <- file.debugReadFully()
      inode2 = file.inode
      (remainingOffset, remainingData) <- file.write(0, w2)
      b <- file.debugReadFully()
    } yield {
      inode2.size should be (e1.length)
      file.inode.size should be (e2.length)
      remainingOffset should be (0)
      remainingData should be (Nil)
      a should be (e1)
      b should be (e2)
    }
  }
  test("Allocate into hole, multi-segment full overwrite with extension") {
    implicit val tx:Transaction = sys.newTransaction()
    val w1 = Array[Byte](5,6,7,8,9,10,11)
    val w2 = Array[Byte](0,1,2,3,4,15,16,17,18,19,20,21,22)
    val e1 = Array[Byte](0,0,0,0,0,5,6,7,8,9,10,11)
    val e2 = Array[Byte](0,1,2,3,4,15,16,17,18,19,20,21,22)
    for {
      file <- boot(Some(5), Some(500))
      (_, _) <- file.write(5, w1)
      a <- file.debugReadFully()
      inode2 = file.inode
      (remainingOffset, remainingData) <- file.write(0, w2)
      b <- file.debugReadFully()
    } yield {
      inode2.size should be (e1.length)
      file.inode.size should be (e2.length)
      remainingOffset should be (0)
      remainingData should be (Nil)
      a should be (e1)
      b should be (e2)
    }
  }
  test("Allocate into hole, full overwrite plus allocation") {
    implicit val tx:Transaction = sys.newTransaction()
    val w1 = Array[Byte](5,6,7)
    val w2 = Array[Byte](0,1,2,3,4,9,9,9,8,9,10,11,12)
    val e1 = Array[Byte](0,0,0,0,0,5,6,7)
    val e2 = Array[Byte](0,1,2,3,4,9,9,9,8,9)
    for {
      file <- boot(Some(5), Some(500))
      (_, _) <- file.write(5, w1)
      a <- file.debugReadFully()
      inode2 = file.inode
      (remainingOffset, remainingData) <- file.write(0, w2)
      b <- file.debugReadFully()
    } yield {
      inode2.size should be (e1.length)
      file.inode.size should be (e2.length)
      remainingOffset should be (10)
      remainingData.foldLeft(0)((sz, db) => sz + db.size) should be (3)
      a should be (e1)
      b should be (e2)
    }
  }
  test("Append to file, partial segment") {
    implicit val tx:Transaction = sys.newTransaction()
    val w1 = Array[Byte](0,1,2)
    val w2 = Array[Byte](3,4)
    val e  = Array[Byte](0,1,2,3,4)
    for {
      file <- boot(Some(5), Some(500))
      (_, _) <- file.write(0, w1)
      inode2 = file.inode
      a <- file.debugReadFully()
      (remainingOffset, remainingData) <- file.write(w1.length, w2)
      b <- file.debugReadFully()
    } yield {
      inode2.size should be (w1.length)
      file.inode.size should be (e.length)
      remainingOffset should be (0)
      remainingData should be (Nil)
      a should be (w1)
      b should be (e)
    }
  }
  test("Append to file, single segment allocation") {
    implicit val tx:Transaction = sys.newTransaction()
    val w1 = Array[Byte](0,1,2)
    val w2 = Array[Byte](3,4,5,6,7)
    val e  = Array[Byte](0,1,2,3,4,5,6,7)
    for {
      file <- boot(Some(5), Some(500))
      (_, _) <- file.write(0, w1)
      inode2 = file.inode
      a <- file.debugReadFully()
      (remainingOffset, remainingData) <- file.write(w1.length, w2)
      b <- file.debugReadFully()
    } yield {
      inode2.size should be (w1.length)
      file.inode.size should be (e.length)
      remainingOffset should be (0)
      remainingData should be (Nil)
      a should be (w1)
      b should be (e)
    }
  }
  test("Append to file, multi segment allocation") {
    implicit val tx:Transaction = sys.newTransaction()
    val w1 = Array[Byte](0,1,2)
    val w2 = Array[Byte](3,4,5,6,7,8,9,10,11,12)
    val e  = Array[Byte](0,1,2,3,4,5,6,7,8,9,10,11,12)
    for {
      file <- boot(Some(5), Some(500))
      (_, _) <- file.write(0, w1)
      inode2 = file.inode
      a <- file.debugReadFully()
      (remainingOffset, remainingData) <- file.write(w1.length, w2)
      b <- file.debugReadFully()
    } yield {
      inode2.size should be (w1.length)
      file.inode.size should be (e.length)
      remainingOffset should be (0)
      remainingData should be (Nil)
      a should be (w1)
      b should be (e)
    }
  }
  test("Append to file, multi segment, multi-buffer allocation") {
    implicit val tx:Transaction = sys.newTransaction()
    val w1 = Array[Byte](0,1,2)
    val w2 = List(Array[Byte](3,4,5,6), Array[Byte](7,8,9,10,11), Array[Byte](12))
    val e  = Array[Byte](0,1,2,3,4,5,6,7,8,9,10,11,12)
    for {
      file <- boot(Some(5), Some(500))
      (_, _) <- file.write(0, w1)
      inode2 = file.inode
      a <- file.debugReadFully()
      (remainingOffset, remainingData) <- file.write(w1.length, w2.map(DataBuffer(_)))
      b <- file.debugReadFully()
    } yield {
      inode2.size should be (w1.length)
      file.inode.size should be (e.length)
      remainingOffset should be (0)
      remainingData should be (Nil)
      a should be (w1)
      b should be (e)
    }
  }
  test("Append to file, three appends") {
    implicit val tx:Transaction = sys.newTransaction()
    val w1 = Array[Byte](0,1,2)
    val w2 = List(Array[Byte](3,4,5,6), Array[Byte](7,8,9,10,11))
    val w3 = Array[Byte](12)
    val e1  = Array[Byte](0,1,2,3,4,5,6,7,8,9,10,11)
    val e2  = Array[Byte](0,1,2,3,4,5,6,7,8,9,10,11,12)
    for {
      file <- boot(Some(5), Some(500))
      (_, _) <- file.write(0, w1)
      inode2 = file.inode
      a <- file.debugReadFully()
      (_, _) <- file.write(w1.length, w2.map(DataBuffer(_)))
      inode3 = file.inode
      b <- file.debugReadFully()
      (remainingOffset, remainingData) <- file.write(12, w3)
      inode4 = file.inode
      c <- file.debugReadFully()
    } yield {
      inode2.size should be (w1.length)
      inode3.size should be (e1.length)
      inode4.size should be (e2.length)
      remainingOffset should be (0)
      remainingData should be (Nil)
      a should be (w1)
      b should be (e1)
      c should be (e2)
    }
  }

  test("Truncate, single segment") {
    implicit val tx:Transaction = sys.newTransaction()
    val w1 = Array[Byte](0,1,2,3)
    val e  = Array[Byte](0,1,2)
    for {
      file <- boot(Some(5), Some(500))
      (_, _) <- file.write(0, w1)
      inode2 = file.inode
      a <- file.debugReadFully()
      fdeleteComplete <- file.truncate(3)
      _ <- fdeleteComplete
      b <- file.debugReadFully()
      _ <- file.fs.getLocalTasksCompleted
    } yield {
      inode2.size should be (w1.length)
      file.inode.size should be (e.length)
      a should be (w1)
      b should be (e)
    }
  }

  test("Truncate, delete segment") {
    implicit val tx:Transaction = sys.newTransaction()
    val w1 = Array[Byte](0,1,2,3,4,5,6,7)
    val e  = Array[Byte](0,1,2)
    for {
      file <- boot(Some(5), Some(500))
      (_, _) <- file.write(0, w1)
      inode2 = file.inode
      a <- file.debugReadFully()
      fdeleteComplete <- file.truncate(3)
      _ <- fdeleteComplete
      b <- file.debugReadFully()
      _ <- file.fs.getLocalTasksCompleted
    } yield {
      inode2.size should be (w1.length)
      file.inode.size should be (e.length)
      a should be (w1)
      b should be (e)
    }
  }
  
  test("Truncate, multi tier") {
    implicit val tx:Transaction = sys.newTransaction()
    val w1 = Array[Byte](0,1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16)
    val e  = Array[Byte](0,1,2)
    for {
      file <- boot(Some(5), Some(120))
      (_, _) <- file.write(0, w1)
      inode2 = file.inode
      a <- file.debugReadFully()
      fdeleteComplete <- file.truncate(3)
      _ <- fdeleteComplete
      b <- file.debugReadFully()
      _ <- file.fs.getLocalTasksCompleted
    } yield {
      inode2.size should be (w1.length)
      file.inode.size should be (e.length)
      a should be (w1)
      b should be (e)
    }
  }
  
  test("Truncate, multi tier to zero") {
    implicit val tx:Transaction = sys.newTransaction()
    val w1 = Array[Byte](0,1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16)
    val e  = Array[Byte]()
    for {
      file <- boot(Some(5), Some(120))
      (_, _) <- file.write(0, w1)
      inode2 = file.inode
      a <- file.debugReadFully()
      fdeleteComplete <- file.truncate(0)
      _ <- fdeleteComplete
      b <- file.debugReadFully()
      _ <- file.fs.getLocalTasksCompleted
    } yield {
      inode2.size should be (w1.length)
      file.inode.size should be (e.length)
      a should be (w1)
      b should be (e)
    }
  }
}