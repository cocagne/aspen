package com.ibm.aspen.cumulofs.impl

import com.ibm.aspen.cumulofs.Directory
import com.ibm.aspen.cumulofs.DirectoryPointer
import com.ibm.aspen.cumulofs.FileSystem
import scala.concurrent.ExecutionContext
import scala.concurrent.Future
import com.ibm.aspen.cumulofs.DirectoryEntry
import com.ibm.aspen.cumulofs.InodePointer
import com.ibm.aspen.cumulofs.DirectoryInode
import com.ibm.aspen.base.tieredlist.SimpleMutableTieredKeyValueList
import com.ibm.aspen.core.objects.keyvalue.ByteArrayKeyOrdering
import com.ibm.aspen.core.objects.keyvalue.Value
import com.ibm.aspen.base.RetryStrategy
import com.ibm.aspen.base.tieredlist.TieredKeyValueList
import com.ibm.aspen.core.objects.KeyValueObjectState
import com.ibm.aspen.core.objects.keyvalue.LexicalKeyOrdering
import com.ibm.aspen.core.transaction.KeyValueUpdate
import com.ibm.aspen.core.objects.keyvalue.Insert
import com.ibm.aspen.base.Transaction
import com.ibm.aspen.base.tieredlist.KeyValueListPointer
import com.ibm.aspen.base.tieredlist.MutableTieredKeyValueList
import com.ibm.aspen.cumulofs.DirectoryNotEmpty
import com.ibm.aspen.cumulofs.FilePointer
import com.ibm.aspen.cumulofs.DeleteFileTask

class SimpleDirectory(
    val pointer: DirectoryPointer,
    val fs: FileSystem) extends Directory {
  
  private[this] var ftl: Option[Future[MutableTieredKeyValueList]] = None
  
  private[this] def tree(implicit ec: ExecutionContext): Future[MutableTieredKeyValueList] = synchronized {
    ftl match {
      case Some(f) => f
      case None => 
        val f = getInode().flatMap(loadTieredList)
        ftl = Some(f)
        f
    }
  }
  
  private[this] def loadTieredList(inode: DirectoryInode)(implicit ec: ExecutionContext): Future[MutableTieredKeyValueList] = inode.contentTree match {
    case Some(root) => Future.successful(createTieredList(root))
    case None => fs.system.retryStrategy.retryUntilSuccessful {
      for {
        // Ensure we have an up-to-date copy of the inode state. This is necessary for retries where some other node may
        // successfully create the tiered list. When that happens we'll simply detect success and skip the creation step
        kvos <- fs.system.readObject(inode.pointer.pointer)
        root <- createDirectoryTable(kvos)
      } yield createTieredList(root)
    }
  }
  
  private[this] def createDirectoryTable(kvos: KeyValueObjectState)(implicit ec: ExecutionContext): Future[TieredKeyValueList.Root] = {
    kvos.contents.get(DirectoryInode.ContentTieredListKey) match {
      case Some(v) => 
        // Some other node must have beaten us to the punch. 
        Future.successful(TieredKeyValueList.Root(v.value))
      
      case None =>
        // Allocate the initial tree object and insert the tiered list root into the directory inode

        fs.system.transact { implicit tx =>
        
          val txreqs = KeyValueUpdate.KVRequirement(DirectoryInode.ContentTieredListKey, tx.timestamp(), KeyValueUpdate.TimestampRequirement.DoesNotExist) :: Nil
          
          for {
            allocater <- fs.system.getObjectAllocater(fs.directoryLoader.dataTableAllocaters(0))
            dirContentPtr <- allocater.allocateKeyValueObject(kvos.pointer, kvos.revision, Nil)
            dirTblRoot = new TieredKeyValueList.Root(0, fs.directoryLoader.dataTableAllocaters, fs.directoryLoader.dataTableSizes, LexicalKeyOrdering, dirContentPtr)
        
            _ = tx.append(kvos.pointer, None, txreqs, Insert(DirectoryInode.ContentTieredListKey, dirTblRoot.toArray(), tx.timestamp()) :: Nil)
          } yield dirTblRoot  
        }
    }
    
  }
  
  def createTieredList( root: TieredKeyValueList.Root ): MutableTieredKeyValueList = new SimpleMutableTieredKeyValueList(
      fs.system, Left(pointer.pointer), DirectoryInode.ContentTieredListKey, ByteArrayKeyOrdering, Some(root))

  def getContents()(implicit ec: ExecutionContext): Future[List[DirectoryEntry]] = {
    var contents: List[DirectoryEntry] = Nil
    
    def visitor(v: Value): Unit = synchronized { contents = DirectoryEntry(v) :: contents }
    
    tree flatMap { tl => tl.visitAll(visitor) } map { _ => contents }
  }
  
  def getEntry(name: String)(implicit ec: ExecutionContext): Future[Option[InodePointer]] = {
    tree flatMap { tl => tl.get(name) } map { o => o.map(v => DirectoryEntry(v).pointer) }
  }
  
  def prepareInsert(name: String, pointer: InodePointer)(implicit tx: Transaction, ec: ExecutionContext): Future[Unit] = {

    val fkvos = fs.system.readObject(pointer.pointer)
    
    for {
      tl <- tree
      kvos <- fkvos
      _ = tx.setRefcount(pointer.pointer, kvos.refcount, kvos.refcount.increment())
      prep <- tl.put(name, pointer.toArray)
    } yield ()
  }
  
  def prepareDelete(name: String)(implicit tx: Transaction, ec: ExecutionContext): Future[Unit] = {
    
    def del(tl: MutableTieredKeyValueList, oentry: Option[InodePointer]): Future[Unit] = oentry match {
      case None => Future.unit // Directory entry not found. We're done!
      
      case Some(inodePtr) => 
        val fdelEntryPrep = tl.delete(name)
        val ftaskPrep     = DeleteFileTask.prepare(fs, inodePtr)
        
        for {
          _ <- fdelEntryPrep
          _ <- ftaskPrep
        } yield ()
    }

    val fentry = lookup(name)
    
    for {
      tl <- tree
      oentry <- fentry
      prepped <- del(tl, oentry)
    } yield ()
  
  }
  
  def prepareForDirectoryDeletion()(implicit tx: Transaction, ec: ExecutionContext): Future[Unit] = {
    
    def prep(rootNode: KeyValueObjectState): Future[Unit] = {
      if (!rootNode.contents.isEmpty)
        Future.failed(new DirectoryNotEmpty(pointer))
      else {
        tx.lockRevision(rootNode.pointer, rootNode.revision)
        Future.unit
      }
    }
    
    for {
      tl <- tree
      node <- tl.fetchMutableNode(KeyValueListPointer.AbsoluteMinimum)
      ready <- prep(node.kvos)
    } yield ()
  }
}