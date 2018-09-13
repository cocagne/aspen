package com.ibm.aspen.cumulofs.impl

import com.ibm.aspen.base.tieredlist.{MutableTieredKeyValueList, TieredKeyValueListNodeAllocaterFactory, TieredKeyValueListRoot}
import com.ibm.aspen.base.{ObjectAllocater, Transaction}
import com.ibm.aspen.core.objects.keyvalue.{Key, LexicalKeyOrdering, Value}
import com.ibm.aspen.core.objects.{KeyValueObjectState, ObjectRevision}
import com.ibm.aspen.cumulofs._
import com.ibm.aspen.cumulofs.error.DirectoryNotEmpty
import com.ibm.aspen.cumulofs.impl.SimpleBaseFile.SimpleSet

import scala.concurrent.{ExecutionContext, Future}

object SimpleDirectory {

  case class SetParentDirectory(newParent: Option[DirectoryPointer]) extends SimpleSet {
    def update(inode: Inode): Inode = inode.asInstanceOf[DirectoryInode].setParentDirectory(newParent)
  }

}

class SimpleDirectory(override val pointer: DirectoryPointer,
                      cachedInodeRevision: ObjectRevision,
                      initialInode: DirectoryInode,
                      fs: FileSystem) extends SimpleBaseFile(pointer, cachedInodeRevision, initialInode, fs) with Directory {


  private[this] var ftl: Option[Future[MutableTieredKeyValueList]] = None
  
  import SimpleDirectory._

  override def inode: DirectoryInode = super.inode.asInstanceOf[DirectoryInode]

  override def inodeState: (DirectoryInode, ObjectRevision) = {
    val t = super.inodeState
    (t._1.asInstanceOf[DirectoryInode], t._2)
  }

  def setParentDirectory(newParent: Option[DirectoryPointer])(implicit ec: ExecutionContext): Future[Unit] = {
    enqueueOp(SetParentDirectory(newParent))
  }
  
  override def refresh()(implicit ec: ExecutionContext): Future[Unit] = synchronized {
    super.refresh()
  }

  private[this] def tree(implicit ec: ExecutionContext): Future[MutableTieredKeyValueList] = synchronized {
    ftl match {
      case Some(f) => f

      case None =>
        inode.ocontents match {
          case Some(tkvl) =>
            val rootMgr = new SimpleDirectoryTLRootManager(fs.system, pointer.pointer, tkvl)
            val f = Future.successful(new MutableTieredKeyValueList(rootMgr))
            ftl = Some(f)
            f

          case None =>
            val (tkvlAllocaterUUID, config) = fs.directoryTableConfig

            val tkvlaFactory = fs.system.typeRegistry.getTypeFactory[TieredKeyValueListNodeAllocaterFactory](tkvlAllocaterUUID).get

            val tkvlAllocater = tkvlaFactory.createNodeAllocater(fs.system, config)

            def alloc(allocater: ObjectAllocater) = fs.system.transact { implicit tx =>
              allocater.allocateKeyValueObject(pointer.pointer, cachedInodeRevision, Nil) map { root =>
                val tkvl = new TieredKeyValueListRoot(0, LexicalKeyOrdering, root, tkvlAllocaterUUID, config)
                val newInode = inode.setContentTree(Some(tkvl))
                tx.overwrite(pointer.pointer, cachedInodeRevision, newInode.toDataBuffer)
                (newInode, tx.txRevision, tkvl)
              }
            }

            val f = for {
              allocater <- tkvlAllocater.tierNodeAllocater(0)
              (newInode, newRevision, tkvl) <- alloc(allocater)
            } yield {
              val rootMgr = new SimpleDirectoryTLRootManager(fs.system, pointer.pointer, tkvl)
              setCachedInode(newInode, newRevision)
              new MutableTieredKeyValueList(rootMgr)
            }

            val fcreate = f.recoverWith { case _ => refresh().flatMap(_ => tree) }

            ftl = Some(fcreate)

            fcreate
        }
    }
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
      _ <- tl.preparePut(name, pointer.toArray)
    } yield ()
  }
  
  def prepareRename(oldName: String, newName: String)(implicit tx: Transaction, ec: ExecutionContext): Future[Unit] = {
    for {
      tl <- tree
      _ <- tl.prepareRename(oldName: String, newName: String)
    } yield ()
  }
  
  def hardLink(name: String, file: BaseFile)(implicit ec: ExecutionContext): Future[Unit] = {
    implicit val tx: Transaction = fs.system.newTransaction()
    
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
      
      if (rootNode.contents.nonEmpty)
        Future.failed(DirectoryNotEmpty(pointer))
      else {
        tx.lockRevision(rootNode.pointer, rootNode.revision)
        tx.setRefcount(rootNode.pointer, rootNode.refcount, rootNode.refcount.decrement())
        Future.unit
      }
    }
    
    for {
      tl <- tree
      node <- tl.fetchMutableNode(Key.AbsoluteMinimum)
      _ <- prep(node.kvos)
    } yield ()
  }
}