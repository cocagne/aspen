package com.ibm.aspen.cumulofs.impl

import com.ibm.aspen.cumulofs.Directory
import com.ibm.aspen.cumulofs.DirectoryPointer
import com.ibm.aspen.cumulofs.FileSystem

import scala.concurrent.ExecutionContext
import scala.concurrent.Future
import com.ibm.aspen.cumulofs.DirectoryEntry
import com.ibm.aspen.cumulofs.InodePointer
import com.ibm.aspen.cumulofs.DirectoryInode
import com.ibm.aspen.base.tieredlist.MutableTieredKeyValueList
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
import com.ibm.aspen.cumulofs.error.DirectoryNotEmpty
import com.ibm.aspen.cumulofs.FilePointer
import com.ibm.aspen.core.objects.keyvalue.Key
import com.ibm.aspen.core.objects.ObjectRevision
import com.ibm.aspen.core.HLCTimestamp
import com.ibm.aspen.cumulofs.Inode
import com.ibm.aspen.core.objects.ObjectRefcount
import com.ibm.aspen.cumulofs.BaseFile
import com.ibm.aspen.base.tieredlist.SimpleTieredKeyValueListNodeAllocater
import com.ibm.aspen.base.tieredlist.TieredKeyValueListRoot
import com.ibm.aspen.base.tieredlist.MutableKeyValueObjectRootManager
import com.ibm.aspen.core.objects.KeyValueObjectPointer

class SimpleDirectory(
    protected var cachedInode: DirectoryInode,
    fs: FileSystem) extends SimpleBaseFile(fs) with Directory {
  
  val pointer: DirectoryPointer = synchronized { cachedInode.pointer }
  
  private[this] var ftl: Option[Future[MutableTieredKeyValueList]] = None
  
  override def updateInode(newRevision: ObjectRevision, newTimestamp: HLCTimestamp, updatedState: Map[Key,Array[Byte]], newRefcount: Option[ObjectRefcount]): Unit = synchronized {
   cachedInode = new DirectoryInode(cachedInode.pointer, newRevision, newRefcount.getOrElse(cachedInode.refcount), newTimestamp, updatedState)
  }
  
  def refresh()(implicit ec: ExecutionContext): Future[Unit] = synchronized {
    fs.inodeLoader.load(cachedInode.pointer).map { refreshedInode => synchronized {
      cachedInode = refreshedInode
    }}
  }
  
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
    case Some(root) => 
      Future.successful(createTieredList(root, inode.pointer.pointer))
      
    case None => fs.system.retryStrategy.retryUntilSuccessful {
      for {
        // Ensure we have an up-to-date copy of the inode state. This is necessary for retries where some other node may
        // successfully create the tiered list. When that happens we'll simply detect success and skip the creation step
        kvos <- fs.system.readObject(inode.pointer.pointer)
        root <- createDirectoryTable(kvos)
      } yield createTieredList(root, kvos.pointer)
    }
  }
  
  private[this] def createDirectoryTable(kvos: KeyValueObjectState)(implicit ec: ExecutionContext): Future[TieredKeyValueListRoot] = {
    kvos.contents.get(DirectoryInode.ContentTieredListKey) match {
      case Some(v) => 
        // Some other node must have beaten us to the punch. 
        Future.successful(TieredKeyValueListRoot(v.value))
      
      case None =>
        // Allocate the initial tree object and insert the tiered list root into the directory inode

        val allocaterType = SimpleTieredKeyValueListNodeAllocater.typeUUID
        val allocaterConfig = SimpleTieredKeyValueListNodeAllocater.encode(fs.directoryLoader.directoryTableAllocaters, 
                                fs.directoryLoader.directoryTableSizes, fs.directoryLoader.directoryTableKVPairLimits)
        
        fs.system.transact { implicit tx =>
        
          val txreqs = KeyValueUpdate.KVRequirement(DirectoryInode.ContentTieredListKey, HLCTimestamp.now, KeyValueUpdate.TimestampRequirement.DoesNotExist) :: Nil
          
          for {
            allocater <- fs.system.getObjectAllocater(fs.directoryLoader.directoryTableAllocaters(0))
            dirContentPtr <- allocater.allocateKeyValueObject(kvos.pointer, kvos.revision, Nil)
            dirTblRoot = TieredKeyValueListRoot(0, LexicalKeyOrdering, dirContentPtr, allocaterType, allocaterConfig)
            
            _ = tx.update(kvos.pointer, None, txreqs, Insert(DirectoryInode.ContentTieredListKey, dirTblRoot.toArray()) :: Nil)
          } yield dirTblRoot  
        }
    }
    
  }
  
  def createTieredList(root: TieredKeyValueListRoot, pointer: KeyValueObjectPointer): MutableTieredKeyValueList = {
    val rootMgr = new MutableKeyValueObjectRootManager(fs.system, pointer, DirectoryInode.ContentTieredListKey, root)
    new MutableTieredKeyValueList(rootMgr)
  }
    

  def getContents()(implicit ec: ExecutionContext): Future[List[DirectoryEntry]] = {
    var contents: List[DirectoryEntry] = Nil
    
    def visitor(v: Value): Unit = synchronized { contents = DirectoryEntry(v) :: contents }
    
    tree flatMap { tl => tl.visitAll(visitor) } map { _ => contents }
  }
  
  def getEntry(name: String)(implicit ec: ExecutionContext): Future[Option[InodePointer]] = {
    tree flatMap { tl => tl.get(name) } map { o => o.map(v => DirectoryEntry(v).pointer) }
  }
  
  def prepareInsert(name: String, pointer: InodePointer, incref: Boolean=true)(implicit tx: Transaction, ec: ExecutionContext): Future[Unit] = {

    val fkvos = fs.system.readObject(pointer.pointer)
    
    for {
      tl <- tree
      kvos <- fkvos
      _ = if (incref) tx.setRefcount(pointer.pointer, kvos.refcount, kvos.refcount.increment())
      prep <- tl.preparePut(name, pointer.toArray)
    } yield ()
  }
  
  def prepareRename(oldName: String, newName: String)(implicit tx: Transaction, ec: ExecutionContext): Future[Unit] = {
    for {
      tl <- tree
      prep <- tl.prepareRename(oldName: String, newName: String)
    } yield ()
  }
  
  def hardLink(name: String, file: BaseFile)(implicit ec: ExecutionContext): Future[Unit] = {
    implicit val tx = fs.system.newTransaction()
    
    prepareInsert(name, file.pointer).flatMap { _ =>tx.commit().map(_=>()) }
  }
  
  def prepareDelete(name: String, decref: Boolean=true)(implicit tx: Transaction, ec: ExecutionContext): Future[Future[Unit]] = {
    
    def del(tl: MutableTieredKeyValueList, oentry: Option[InodePointer]): Future[Future[Unit]] = oentry match {
      case None => Future.successful(Future.unit) // Directory entry not found. We're done!
      
      case Some(inodePtr) => 
        val fdelEntryPrep = tl.prepareDelete(name)
        
        if (decref) {
          val ftaskPrep = DeleteFileTask.prepare(fs, inodePtr)
          
          for {
            _ <- fdelEntryPrep
            fcomplete <- ftaskPrep
          } yield fcomplete
        } else {
          fdelEntryPrep.map(_=>Future.unit)
        }
    }

    val fentry = lookup(name)
    
    for {
      tl <- tree
      oentry <- fentry
      fcomplete <- del(tl, oentry)
    } yield fcomplete
  
  }
    

  /** This prevents entries from being added to the directory while the delete is in progress. The
   *  directory must be empty so the tier0 list will contain only a single element. Deleting that
   *  node is made part of the directory deletion transaction. Thus either the other writer will win
   *  and the directory deletion will fail or we'll win and the other writer's directory insertion will
   *  fail. 
   */
  def prepareForDirectoryDeletion()(implicit tx: Transaction, ec: ExecutionContext): Future[Unit] = {
    
    def prep(rootNode: KeyValueObjectState): Future[Unit] = {
      
      if (!rootNode.contents.isEmpty) 
        Future.failed(new DirectoryNotEmpty(pointer))
      else {
        tx.lockRevision(rootNode.pointer, rootNode.revision)
        tx.setRefcount(rootNode.pointer, rootNode.refcount, rootNode.refcount.decrement())
        Future.unit
      }
    }
    
    for {
      tl <- tree
      node <- tl.fetchMutableNode(Key.AbsoluteMinimum)
      ready <- prep(node.kvos)
    } yield ()
  }
}