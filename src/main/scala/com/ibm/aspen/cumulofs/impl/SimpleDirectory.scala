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
import com.ibm.aspen.cumulofs.error.DirectoryNotEmpty
import com.ibm.aspen.cumulofs.FilePointer
import com.ibm.aspen.cumulofs.DeleteFileTask
import com.ibm.aspen.core.objects.keyvalue.Key
import com.ibm.aspen.core.objects.ObjectRevision
import com.ibm.aspen.core.HLCTimestamp
import com.ibm.aspen.cumulofs.Inode
import com.ibm.aspen.core.objects.ObjectRefcount
import com.ibm.aspen.cumulofs.BaseFile

class SimpleDirectory(
    protected var inode: DirectoryInode,
    fs: FileSystem) extends SimpleBaseFile(fs) with Directory {
  
  val pointer: DirectoryPointer = inode.pointer
  
  private[this] var ftl: Option[Future[MutableTieredKeyValueList]] = None
  
  override protected def updateInode(newRevision: ObjectRevision, newTimestamp: HLCTimestamp, updatedState: Map[Key,Value], newRefcount: Option[ObjectRefcount]): Unit = {
   inode = new DirectoryInode(inode.pointer, newRevision, newRefcount.getOrElse(inode.refcount), newTimestamp, updatedState)
  }
  
  def refresh()(implicit ec: ExecutionContext): Future[Unit] = synchronized {
    fs.inodeLoader.load(inode.pointer).map { refreshedInode => synchronized {
      inode = refreshedInode
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
            allocater <- fs.system.getObjectAllocater(fs.directoryLoader.directoryTableAllocaters(0))
            dirContentPtr <- allocater.allocateKeyValueObject(kvos.pointer, kvos.revision, Nil)
            dirTblRoot = new TieredKeyValueList.Root(0, fs.directoryLoader.directoryTableAllocaters, fs.directoryLoader.directoryTableSizes, LexicalKeyOrdering, dirContentPtr)
        
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
  
  def prepareInsert(name: String, pointer: InodePointer, incref: Boolean=true)(implicit tx: Transaction, ec: ExecutionContext): Future[Unit] = {

    val fkvos = fs.system.readObject(pointer.pointer)
    
    for {
      tl <- tree
      kvos <- fkvos
      _ = if (incref) tx.setRefcount(pointer.pointer, kvos.refcount, kvos.refcount.increment())
      prep <- tl.put(name, pointer.toArray)
    } yield ()
  }
  
  def prepareRename(oldName: String, newName: String)(implicit tx: Transaction, ec: ExecutionContext): Future[Unit] = {
    for {
      tl <- tree
      prep <- tl.replace(oldName: String, newName: String)
    } yield ()
  }
  
  def hardLink(name: String, file: BaseFile)(implicit ec: ExecutionContext): Future[Unit] = {
    implicit val tx = fs.system.newTransaction()
    
    prepareInsert(name, file.pointer).flatMap { _ =>tx.commit() }
  }
  
  def prepareDelete(name: String, decref: Boolean=true)(implicit tx: Transaction, ec: ExecutionContext): Future[Unit] = {
    println(s"** DOING DELETE of $name with decref $decref")
    def del(tl: MutableTieredKeyValueList, oentry: Option[InodePointer]): Future[Unit] = oentry match {
      case None => Future.unit // Directory entry not found. We're done!
      
      case Some(inodePtr) => 
        val fdelEntryPrep = tl.delete(name)
        println(s"** prepping delete")
        if (decref) {
          println(s"** prepping task")
          val ftaskPrep     = DeleteFileTask.prepare(fs, inodePtr)
          
          for {
            _ <- fdelEntryPrep
            _ <- ftaskPrep
          } yield ()
        } else {
          fdelEntryPrep
        }
    }

    val fentry = lookup(name)
    
    for {
      tl <- tree
      oentry <- fentry
      prepped <- del(tl, oentry)
    } yield ()
  
  }
  
  override def freeResources()(implicit ec: ExecutionContext): Future[Unit] = {
    for {
      tl <- tree
      node <- tl.fetchMutableNode(Key.AbsoluteMinimum)
      _ = if (node.kvos.contents.isEmpty) throw new DirectoryNotEmpty(pointer)
      done <- tree.flatMap(_.destroy( _ => Future.unit ))
    } yield ()
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