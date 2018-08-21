package com.ibm.aspen.cumulofs.impl

import com.ibm.aspen.base.TestSystemSuite
import scala.concurrent.Future
import com.ibm.aspen.cumulofs.FileSystem
import com.ibm.aspen.cumulofs.FileInode
import com.ibm.aspen.cumulofs.impl.IndexedFileContent.WriteStatus
import com.ibm.aspen.core.DataBuffer
import com.ibm.aspen.core.objects.keyvalue.KeyValueOperation
import com.ibm.aspen.util.Varint
import com.ibm.aspen.core.objects.keyvalue.Value
import com.ibm.aspen.core.objects.DataObjectPointer
import com.ibm.aspen.core.objects.keyvalue.Key
import com.ibm.aspen.base.task.LocalTaskGroup
import com.ibm.aspen.core.objects.keyvalue.Insert

object IndexedFileContentSuite {
  
}

class IndexedFileContentSuite extends TestSystemSuite with CumuloFSBootstrap {
  import IndexedFileContentSuite._
  
  def write(inode: FileInode, ifc: IndexedFileContent, offset: Long, bytes: Array[Int]): Future[(FileInode, Long, List[DataBuffer])] = {
    write(inode, ifc, offset, bytes.map(i => i.asInstanceOf[Byte]))
  }
  
  def write(inode: FileInode, ifc: IndexedFileContent, offset: Long, bytes: Array[Byte]): Future[(FileInode, Long, List[DataBuffer])] = {
    write(inode, ifc, offset, List(bytes))
  }
    
  def write(inode: FileInode, ifc: IndexedFileContent, offset: Long, lbytes: List[Array[Byte]]): Future[(FileInode, Long, List[DataBuffer])] = {
    implicit val tx = sys.newTransaction()
    
    val nbytes = lbytes.foldLeft(0)((sz, a) => sz + a.length)
    
    val newSize = if (offset + nbytes > inode.size) offset + nbytes else inode.size
    
    def getNewContent(newRoot: Option[DataObjectPointer]): Map[Key,Array[Byte]] = {
      val base = inode.content + 
        (FileInode.FileSizeKey -> Varint.unsignedLongToArray(newSize))
      newRoot match {
        case None => base
        case Some(p) => base + (FileInode.FileIndexRootKey -> p.toArray) 
      }
    }
    
    for { 
      ws <- ifc.write(inode, offset, lbytes.map(a => DataBuffer(a)))
      newContent = getNewContent(ws.newRoot)
      _ = tx.update(inode.pointer.pointer, Some(inode.revision), Nil, newContent.toList.map(t => Insert(t._1, t._2)))
      timestamp <- tx.commit()
      _ <- ws.writeComplete
    } yield {
      val adjustedSize = newSize - ws.remainingData.foldLeft(0)((sz, db) => sz + db.size)
      (inode.update(tx.txRevision, timestamp, adjustedSize, ws.newRoot.map(_.toArray), inode.refcount), ws.remainingOffset, ws.remainingData)
    }
  }
  
  def truncate(inode: FileInode, ifc: IndexedFileContent, offset: Long): Future[FileInode] = {
    implicit val tx = sys.newTransaction()
    
    def getNewContent(newRoot: Option[DataObjectPointer]): Map[Key,Array[Byte]] = {
      val base = inode.content + 
        (FileInode.FileSizeKey -> Varint.unsignedLongToArray(offset))
      newRoot match {
        case None => base
        case Some(p) => base + (FileInode.FileIndexRootKey -> p.toArray) 
      }
    }
    
    for { 
      ws <- ifc.truncate(inode, offset)
      newContent = getNewContent(ws.newRoot)
      _ = tx.update(inode.pointer.pointer, Some(inode.revision), Nil, newContent.toList.map(t => Insert(t._1, t._2)))
      timestamp <- tx.commit()
      _ <- ws.writeComplete
    } yield {
      inode.update(tx.txRevision, timestamp, offset, ws.newRoot.map(_.toArray), inode.refcount)
    }
  }
  
  def boot(): Future[(FileSystem, FileInode)] = for {
    fs <- bootstrap()
    rootDir <- fs.loadRoot()
    newFilePointer <- rootDir.createFile("foo", mode=0, uid=1, gid=2)
    newInode <- fs.inodeLoader.load(newFilePointer)
  } yield (fs, newInode)
  
  test("Read empty File") {
    for {
      (fs, newInode) <- boot()
      ifc = new IndexedFileContent(fs, newInode, Some(5), Some(500))
      a <- ifc.debugReadFully(newInode)
    } yield {
      a.length should be (0)
    }
  }
  
  test("Read write empty file") {
    implicit val tx = sys.newTransaction()
    val w = Array[Byte](5)
    for {
      (fs, inode) <- boot()
      ifc = new IndexedFileContent(fs, inode, Some(5), Some(500))
      (inode2, remainingOffset, remainingData) <- write(inode, ifc, 0, w)
      a <- ifc.debugReadFully(inode2)
    } yield {
      inode2.size should be (w.length)
      remainingOffset should be (0)
      remainingData should be (Nil)
      a should be (w)
    }
  }
  
  test("Read write empty file with hole") {
    implicit val tx = sys.newTransaction()
    val w = Array[Byte](5)
    val e = Array[Byte](0,0,5)
    for {
      (fs, inode) <- boot()
      ifc = new IndexedFileContent(fs, inode, Some(5), Some(500))
      (inode2, remainingOffset, remainingData) <- write(inode, ifc, 2, w)
      a <- ifc.debugReadFully(inode2)
    } yield {
      inode2.size should be (3)
      remainingOffset should be (0)
      remainingData should be (Nil)
      a should be (e)
    }
  }
  
  test("Read write empty file with full segment hole") {
    implicit val tx = sys.newTransaction()
    val w = Array[Byte](5)
    val e = Array[Byte](0,0,0,0,0,0,5)
    for {
      (fs, inode) <- boot()
      ifc = new IndexedFileContent(fs, inode, Some(5), Some(500))
      (inode2, remainingOffset, remainingData) <- write(inode, ifc, 6, w)
      a <- ifc.debugReadFully(inode2)
    } yield {
      inode2.size should be (7)
      remainingOffset should be (0)
      remainingData should be (Nil)
      a should be (e)
    }
  }
  
  test("Partial read, single segment") {
    implicit val tx = sys.newTransaction()
    val w = Array[Byte](1,2,3,4,5,6,7,8,9)
    for {
      (fs, inode) <- boot()
      ifc = new IndexedFileContent(fs, inode, Some(5), Some(500))
      (inode2, remainingOffset, remainingData) <- write(inode, ifc, 0, w)
      a <- ifc.read(inode2,2,2)
    } yield {
      inode2.size should be (w.length)
      remainingOffset should be (0)
      remainingData should be (Nil)
      a.get.getByteArray() should be (Array[Byte](3,4))
    }
  }
  test("Partial read, multi segment") {
    implicit val tx = sys.newTransaction()
    val w = Array[Byte](1,2,3,4,5,6,7,8,9)
    for {
      (fs, inode) <- boot()
      ifc = new IndexedFileContent(fs, inode, Some(5), Some(500))
      (inode2, remainingOffset, remainingData) <- write(inode, ifc, 0, w)
      a <- ifc.read(inode2,2,5)
    } yield {
      inode2.size should be (w.length)
      remainingOffset should be (0)
      remainingData should be (Nil)
      a.get.getByteArray() should be (Array[Byte](3,4,5,6,7))
    }
  }
  
  test("Write two segments") {
    implicit val tx = sys.newTransaction()
    val w = Array[Byte](1,2,3,4,5,6,7,8,9)
    for {
      (fs, inode) <- boot()
      ifc = new IndexedFileContent(fs, inode, Some(5), Some(500))
      (inode2, remainingOffset, remainingData) <- write(inode, ifc, 0, w)
      a <- ifc.debugReadFully(inode2)
    } yield {
      inode2.size should be (w.length)
      remainingOffset should be (0)
      remainingData should be (Nil)
      a should be (w)
    }
  }
  
  test("Write three segments") {
    implicit val tx = sys.newTransaction()
    val w = Array[Byte](1,2,3,4,5,6,7,8,9,10,11)
    for {
      (fs, inode) <- boot()
      ifc = new IndexedFileContent(fs, inode, Some(5), Some(500))
      (inode2, remainingOffset, remainingData) <- write(inode, ifc, 0, w)
      a <- ifc.debugReadFully(inode2)
    } yield {
      inode2.size should be (w.length)
      remainingOffset should be (0)
      remainingData should be (Nil)
      a should be (w)
    }
  }
  
  test("Write emtpy to multi-tier index") {
    implicit val tx = sys.newTransaction()
    val w = Array[Byte](1,2,3,4,5,6,7,8,9,10,11,13,14,15,16)
    for {
      (fs, inode) <- boot()
      ifc = new IndexedFileContent(fs, inode, Some(5), Some(120))
      (inode2, remainingOffset, remainingData) <- write(inode, ifc, 0, w)
      a <- ifc.debugReadFully(inode2)
    } yield {
      inode2.size should be (w.length)
      remainingOffset should be (0)
      remainingData should be (Nil)
      a should be (w)
    }
  }
  
  test("Multi-buffer write") {
    implicit val tx = sys.newTransaction()
    val a = Array[Byte](5,6)
    val b = Array[Byte](7,8)
    val w = Array[Byte](5,6,7,8)
    for {
      (fs, inode) <- boot()
      ifc = new IndexedFileContent(fs, inode, Some(5), Some(500))
      (inode2, remainingOffset, remainingData) <- write(inode, ifc, 0, List(a,b))
      a <- ifc.debugReadFully(inode2)
    } yield {
      inode2.size should be (w.length)
      remainingOffset should be (0)
      remainingData should be (Nil)
      a should be (w)
    }
  }
  
  test("Overwrite single segment, no extend") {
    implicit val tx = sys.newTransaction()
    val w1 = Array[Byte](0,1,2,3)
    val w2 = Array[Byte](4,5,6,7)
    for {
      (fs, inode) <- boot()
      ifc = new IndexedFileContent(fs, inode, Some(5), Some(500))
      (inode2, remainingOffset, remainingData) <- write(inode, ifc, 0, w1)
      a <- ifc.debugReadFully(inode2)
      (inode3, remainingOffset, remainingData) <- write(inode2, ifc, 0, w2)
      b <- ifc.debugReadFully(inode3)
    } yield {
      inode2.size should be (w1.length)
      remainingOffset should be (0)
      remainingData should be (Nil)
      a should be (w1)
      b should be (w2)
    }
  }
  
  test("Overwrite single segment, with extend") {
    implicit val tx = sys.newTransaction()
    val w1 = Array[Byte](0,1)
    val w2 = Array[Byte](4,5,6,7)
    for {
      (fs, inode) <- boot()
      ifc = new IndexedFileContent(fs, inode, Some(5), Some(500))
      (inode2, remainingOffset, remainingData) <- write(inode, ifc, 0, w1)
      a <- ifc.debugReadFully(inode2)
      (inode3, remainingOffset, remainingData) <- write(inode2, ifc, 0, w2)
      b <- ifc.debugReadFully(inode3)
    } yield {
      inode2.size should be (w1.length)
      inode3.size should be (w2.length)
      remainingOffset should be (0)
      remainingData should be (Nil)
      a should be (w1)
      b should be (w2)
    }
  }
  test("Overwrite segment with allocation leaves remaining data") {
    implicit val tx = sys.newTransaction()
    val w1 = Array[Byte](0,1)
    val w2 = Array[Byte](0,1,2,3,4,5,6)
    val e = Array[Byte](0,1,2,3,4)
    for {
      (fs, inode) <- boot()
      ifc = new IndexedFileContent(fs, inode, Some(5), Some(500))
      (inode2, remainingOffset, remainingData) <- write(inode, ifc, 0, w1)
      a <- ifc.debugReadFully(inode2)
      (inode3, remainingOffset, remainingData) <- write(inode2, ifc, 0, w2)
      b <- ifc.debugReadFully(inode3)
    } yield {
      inode2.size should be (w1.length)
      inode3.size should be (5)
      remainingOffset should be (5)
      remainingData.foldLeft(0)((sz, db) => sz + db.size) should be (2)
      a should be (w1)
      b should be (e)
    }
  }
  test("Allocate into hole, short segment") {
    implicit val tx = sys.newTransaction()
    val w1 = Array[Byte](5)
    val w2 = Array[Byte](0,1,2,3)
    val e1 = Array[Byte](0,0,0,0,0,5)
    val e2 = Array[Byte](0,1,2,3,0,5)
    for {
      (fs, inode) <- boot()
      ifc = new IndexedFileContent(fs, inode, Some(5), Some(500))
      (inode2, remainingOffset, remainingData) <- write(inode, ifc, 5, w1)
      a <- ifc.debugReadFully(inode2)
      (inode3, remainingOffset, remainingData) <- write(inode2, ifc, 0, w2)
      b <- ifc.debugReadFully(inode3)
    } yield {
      inode2.size should be (6)
      inode3.size should be (6)
      remainingOffset should be (0)
      remainingData should be (Nil)
      a should be (e1)
      b should be (e2)
    }
  }
  test("Allocate into hole, full segment") {
    implicit val tx = sys.newTransaction()
    val w1 = Array[Byte](5)
    val w2 = Array[Byte](0,1,2,3,4)
    val e1 = Array[Byte](0,0,0,0,0,5)
    val e2 = Array[Byte](0,1,2,3,4,5)
    for {
      (fs, inode) <- boot()
      ifc = new IndexedFileContent(fs, inode, Some(5), Some(500))
      (inode2, remainingOffset, remainingData) <- write(inode, ifc, 5, w1)
      a <- ifc.debugReadFully(inode2)
      (inode3, remainingOffset, remainingData) <- write(inode2, ifc, 0, w2)
      b <- ifc.debugReadFully(inode3)
    } yield {
      inode2.size should be (6)
      inode3.size should be (6)
      remainingOffset should be (0)
      remainingData should be (Nil)
      a should be (e1)
      b should be (e2)
    }
  }
  test("Allocate into hole, multi-segment") {
    implicit val tx = sys.newTransaction()
    val w1 = Array[Byte](10)
    val w2 = Array[Byte](0,1,2,3,4,5,6,7,8,9)
    val e1 = Array[Byte](0,0,0,0,0,0,0,0,0,0,10)
    val e2 = Array[Byte](0,1,2,3,4,5,6,7,8,9,10)
    for {
      (fs, inode) <- boot()
      ifc = new IndexedFileContent(fs, inode, Some(5), Some(500))
      (inode2, remainingOffset, remainingData) <- write(inode, ifc, 10, w1)
      a <- ifc.debugReadFully(inode2)
      (inode3, remainingOffset, remainingData) <- write(inode2, ifc, 0, w2)
      b <- ifc.debugReadFully(inode3)
    } yield {
      inode2.size should be (e1.length)
      inode3.size should be (e2.length)
      remainingOffset should be (0)
      remainingData should be (Nil)
      a should be (e1)
      b should be (e2)
    }
  }
  test("Allocate into hole, partial overwrite") {
    implicit val tx = sys.newTransaction()
    val w1 = Array[Byte](5,6,7)
    val w2 = Array[Byte](0,1,2,3,4,9,9)
    val e1 = Array[Byte](0,0,0,0,0,5,6,7)
    val e2 = Array[Byte](0,1,2,3,4,9,9,7)
    for {
      (fs, inode) <- boot()
      ifc = new IndexedFileContent(fs, inode, Some(5), Some(500))
      (inode2, remainingOffset, remainingData) <- write(inode, ifc, 5, w1)
      a <- ifc.debugReadFully(inode2)
      (inode3, remainingOffset, remainingData) <- write(inode2, ifc, 0, w2)
      b <- ifc.debugReadFully(inode3)
    } yield {
      inode2.size should be (e1.length)
      inode3.size should be (e2.length)
      remainingOffset should be (0)
      remainingData should be (Nil)
      a should be (e1)
      b should be (e2)
    }
  }
  test("Allocate into hole, full overwrite") {
    implicit val tx = sys.newTransaction()
    val w1 = Array[Byte](5,6,7)
    val w2 = Array[Byte](0,1,2,3,4,9,9,9)
    val e1 = Array[Byte](0,0,0,0,0,5,6,7)
    val e2 = Array[Byte](0,1,2,3,4,9,9,9)
    for {
      (fs, inode) <- boot()
      ifc = new IndexedFileContent(fs, inode, Some(5), Some(500))
      (inode2, remainingOffset, remainingData) <- write(inode, ifc, 5, w1)
      a <- ifc.debugReadFully(inode2)
      (inode3, remainingOffset, remainingData) <- write(inode2, ifc, 0, w2)
      b <- ifc.debugReadFully(inode3)
    } yield {
      inode2.size should be (e1.length)
      inode3.size should be (e2.length)
      remainingOffset should be (0)
      remainingData should be (Nil)
      a should be (e1)
      b should be (e2)
    }
  }
  test("Allocate into hole, multi-segment partial overwrite") {
    implicit val tx = sys.newTransaction()
    val w1 = Array[Byte](5,6,7,8,9,10,11)
    val w2 = Array[Byte](0,1,2,3,4,15,16,17,18,19,20)
    val e1 = Array[Byte](0,0,0,0,0,5,6,7,8,9,10,11)
    val e2 = Array[Byte](0,1,2,3,4,15,16,17,18,19,20,11)
    for {
      (fs, inode) <- boot()
      ifc = new IndexedFileContent(fs, inode, Some(5), Some(500))
      (inode2, remainingOffset, remainingData) <- write(inode, ifc, 5, w1)
      a <- ifc.debugReadFully(inode2)
      (inode3, remainingOffset, remainingData) <- write(inode2, ifc, 0, w2)
      b <- ifc.debugReadFully(inode3)
    } yield {
      inode2.size should be (e1.length)
      inode3.size should be (e2.length)
      remainingOffset should be (0)
      remainingData should be (Nil)
      a should be (e1)
      b should be (e2)
    }
  }
  test("Allocate into hole, multi-segment full overwrite") {
    implicit val tx = sys.newTransaction()
    val w1 = Array[Byte](5,6,7,8,9,10,11)
    val w2 = Array[Byte](0,1,2,3,4,15,16,17,18,19,20,21)
    val e1 = Array[Byte](0,0,0,0,0,5,6,7,8,9,10,11)
    val e2 = Array[Byte](0,1,2,3,4,15,16,17,18,19,20,21)
    for {
      (fs, inode) <- boot()
      ifc = new IndexedFileContent(fs, inode, Some(5), Some(500))
      (inode2, remainingOffset, remainingData) <- write(inode, ifc, 5, w1)
      a <- ifc.debugReadFully(inode2)
      (inode3, remainingOffset, remainingData) <- write(inode2, ifc, 0, w2)
      b <- ifc.debugReadFully(inode3)
    } yield {
      inode2.size should be (e1.length)
      inode3.size should be (e2.length)
      remainingOffset should be (0)
      remainingData should be (Nil)
      a should be (e1)
      b should be (e2)
    }
  }
  test("Allocate into hole, multi-segment full overwrite with extension") {
    implicit val tx = sys.newTransaction()
    val w1 = Array[Byte](5,6,7,8,9,10,11)
    val w2 = Array[Byte](0,1,2,3,4,15,16,17,18,19,20,21,22)
    val e1 = Array[Byte](0,0,0,0,0,5,6,7,8,9,10,11)
    val e2 = Array[Byte](0,1,2,3,4,15,16,17,18,19,20,21,22)
    for {
      (fs, inode) <- boot()
      ifc = new IndexedFileContent(fs, inode, Some(5), Some(500))
      (inode2, remainingOffset, remainingData) <- write(inode, ifc, 5, w1)
      a <- ifc.debugReadFully(inode2)
      (inode3, remainingOffset, remainingData) <- write(inode2, ifc, 0, w2)
      b <- ifc.debugReadFully(inode3)
    } yield {
      inode2.size should be (e1.length)
      inode3.size should be (e2.length)
      remainingOffset should be (0)
      remainingData should be (Nil)
      a should be (e1)
      b should be (e2)
    }
  }
  test("Allocate into hole, full overwrite plus allocation") {
    implicit val tx = sys.newTransaction()
    val w1 = Array[Byte](5,6,7)
    val w2 = Array[Byte](0,1,2,3,4,9,9,9,8,9,10,11,12)
    val e1 = Array[Byte](0,0,0,0,0,5,6,7)
    val e2 = Array[Byte](0,1,2,3,4,9,9,9,8,9)
    for {
      (fs, inode) <- boot()
      ifc = new IndexedFileContent(fs, inode, Some(5), Some(500))
      (inode2, remainingOffset, remainingData) <- write(inode, ifc, 5, w1)
      a <- ifc.debugReadFully(inode2)
      (inode3, remainingOffset, remainingData) <- write(inode2, ifc, 0, w2)
      b <- ifc.debugReadFully(inode3)
    } yield {
      inode2.size should be (e1.length)
      inode3.size should be (e2.length)
      remainingOffset should be (10)
      remainingData.foldLeft(0)((sz, db) => sz + db.size) should be (3)
      a should be (e1)
      b should be (e2)
    }
  }
  test("Append to file, partial segment") {
    implicit val tx = sys.newTransaction()
    val w1 = Array[Byte](0,1,2)
    val w2 = Array[Byte](3,4)
    val e  = Array[Byte](0,1,2,3,4)
    for {
      (fs, inode) <- boot()
      ifc = new IndexedFileContent(fs, inode, Some(5), Some(500))
      (inode2, remainingOffset, remainingData) <- write(inode, ifc, 0, w1)
      a <- ifc.debugReadFully(inode2)
      (inode3, remainingOffset, remainingData) <- write(inode2, ifc, w1.length, w2)
      b <- ifc.debugReadFully(inode3)
    } yield {
      inode2.size should be (w1.length)
      inode3.size should be (e.length)
      remainingOffset should be (0)
      remainingData should be (Nil)
      a should be (w1)
      b should be (e)
    }
  }
  test("Append to file, single segment allocation") {
    implicit val tx = sys.newTransaction()
    val w1 = Array[Byte](0,1,2)
    val w2 = Array[Byte](3,4,5,6,7)
    val e  = Array[Byte](0,1,2,3,4,5,6,7)
    for {
      (fs, inode) <- boot()
      ifc = new IndexedFileContent(fs, inode, Some(5), Some(500))
      (inode2, remainingOffset, remainingData) <- write(inode, ifc, 0, w1)
      a <- ifc.debugReadFully(inode2)
      (inode3, remainingOffset, remainingData) <- write(inode2, ifc, w1.length, w2)
      b <- ifc.debugReadFully(inode3)
    } yield {
      inode2.size should be (w1.length)
      inode3.size should be (e.length)
      remainingOffset should be (0)
      remainingData should be (Nil)
      a should be (w1)
      b should be (e)
    }
  }
  test("Append to file, multi segment allocation") {
    implicit val tx = sys.newTransaction()
    val w1 = Array[Byte](0,1,2)
    val w2 = Array[Byte](3,4,5,6,7,8,9,10,11,12)
    val e  = Array[Byte](0,1,2,3,4,5,6,7,8,9,10,11,12)
    for {
      (fs, inode) <- boot()
      ifc = new IndexedFileContent(fs, inode, Some(5), Some(500))
      (inode2, remainingOffset, remainingData) <- write(inode, ifc, 0, w1)
      a <- ifc.debugReadFully(inode2)
      (inode3, remainingOffset, remainingData) <- write(inode2, ifc, w1.length, w2)
      b <- ifc.debugReadFully(inode3)
    } yield {
      inode2.size should be (w1.length)
      inode3.size should be (e.length)
      remainingOffset should be (0)
      remainingData should be (Nil)
      a should be (w1)
      b should be (e)
    }
  }
  test("Append to file, multi segment, multi-buffer allocation") {
    implicit val tx = sys.newTransaction()
    val w1 = Array[Byte](0,1,2)
    val w2 = List(Array[Byte](3,4,5,6), Array[Byte](7,8,9,10,11), Array[Byte](12))
    val e  = Array[Byte](0,1,2,3,4,5,6,7,8,9,10,11,12)
    for {
      (fs, inode) <- boot()
      ifc = new IndexedFileContent(fs, inode, Some(5), Some(500))
      (inode2, remainingOffset, remainingData) <- write(inode, ifc, 0, w1)
      a <- ifc.debugReadFully(inode2)
      (inode3, remainingOffset, remainingData) <- write(inode2, ifc, w1.length, w2)
      b <- ifc.debugReadFully(inode3)
    } yield {
      inode2.size should be (w1.length)
      inode3.size should be (e.length)
      remainingOffset should be (0)
      remainingData should be (Nil)
      a should be (w1)
      b should be (e)
    }
  }
  test("Append to file, three appends") {
    implicit val tx = sys.newTransaction()
    val w1 = Array[Byte](0,1,2)
    val w2 = List(Array[Byte](3,4,5,6), Array[Byte](7,8,9,10,11))
    val w3 = Array[Byte](12)
    val e1  = Array[Byte](0,1,2,3,4,5,6,7,8,9,10,11)
    val e2  = Array[Byte](0,1,2,3,4,5,6,7,8,9,10,11,12)
    for {
      (fs, inode) <- boot()
      ifc = new IndexedFileContent(fs, inode, Some(5), Some(500))
      (inode2, remainingOffset, remainingData) <- write(inode, ifc, 0, w1)
      a <- ifc.debugReadFully(inode2)
      (inode3, remainingOffset, remainingData) <- write(inode2, ifc, w1.length, w2)
      b <- ifc.debugReadFully(inode3)
      (inode4, remainingOffset, remainingData) <- write(inode3, ifc, 12, w3)
      c <- ifc.debugReadFully(inode4)
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
    implicit val tx = sys.newTransaction()
    val w1 = Array[Byte](0,1,2,3)
    val e  = Array[Byte](0,1,2)
    for {
      (fs, inode) <- boot()
      ifc = new IndexedFileContent(fs, inode, Some(5), Some(500))
      (inode2, _, _) <- write(inode, ifc, 0, w1)
      a <- ifc.debugReadFully(inode2)
      inode3 <- truncate(inode2, ifc, 3)
      b <- ifc.debugReadFully(inode3)
      _ <- fs.getLocalTasksCompleted()
    } yield {
      inode2.size should be (w1.length)
      inode3.size should be (e.length)
      a should be (w1)
      b should be (e)
    }
  }
  
  test("Truncate, delete segment") {
    implicit val tx = sys.newTransaction()
    val w1 = Array[Byte](0,1,2,3,4,5,6,7)
    val e  = Array[Byte](0,1,2)
    for {
      (fs, inode) <- boot()
      ifc = new IndexedFileContent(fs, inode, Some(5), Some(500))
      (inode2, _, _) <- write(inode, ifc, 0, w1)
      a <- ifc.debugReadFully(inode2)
      inode3 <- truncate(inode2, ifc, 3)
      b <- ifc.debugReadFully(inode3)
      _ <- fs.getLocalTasksCompleted()
    } yield {
      inode2.size should be (w1.length)
      inode3.size should be (e.length)
      a should be (w1)
      b should be (e)
    }
  }
  
  test("Truncate, multi tier") {
    implicit val tx = sys.newTransaction()
    val w1 = Array[Byte](0,1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16)
    val e  = Array[Byte](0,1,2)
    for {
      (fs, inode) <- boot()
      ifc = new IndexedFileContent(fs, inode, Some(5), Some(120))
      (inode2, _, _) <- write(inode, ifc, 0, w1)
      a <- ifc.debugReadFully(inode2)
      inode3 <- truncate(inode2, ifc, 3)
      b <- ifc.debugReadFully(inode3)
      _ <- fs.getLocalTasksCompleted()
    } yield {
      inode2.size should be (w1.length)
      inode3.size should be (e.length)
      a should be (w1)
      b should be (e)
    }
  }
  
  test("Truncate, multi tier to zero") {
    implicit val tx = sys.newTransaction()
    val w1 = Array[Byte](0,1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16)
    val e  = Array[Byte]()
    for {
      (fs, inode) <- boot()
      ifc = new IndexedFileContent(fs, inode, Some(5), Some(120))
      (inode2, _, _) <- write(inode, ifc, 0, w1)
      a <- ifc.debugReadFully(inode2)
      inode3 <- truncate(inode2, ifc, 0)
      b <- ifc.debugReadFully(inode3)
      _ <- fs.getLocalTasksCompleted()
    } yield {
      inode2.size should be (w1.length)
      inode3.size should be (e.length)
      a should be (w1)
      b should be (e)
    }
  }
}