package com.ibm.aspen.cumulofs.impl

import com.ibm.aspen.base.tieredlist.{MutableTieredKeyValueList, TieredKeyValueListNodeAllocaterFactory, TieredKeyValueListRoot}
import com.ibm.aspen.base.{ObjectAllocater, Transaction}
import com.ibm.aspen.core.HLCTimestamp
import com.ibm.aspen.core.objects.keyvalue.{Key, LexicalKeyOrdering, Value}
import com.ibm.aspen.core.objects.{KeyValueObjectState, ObjectRevision}
import com.ibm.aspen.core.transaction.KeyValueUpdate
import com.ibm.aspen.cumulofs._
import com.ibm.aspen.cumulofs.error.DirectoryNotEmpty
import com.ibm.aspen.cumulofs.impl.SimpleBaseFile.SimpleSet

import scala.concurrent.{ExecutionContext, Future, Promise}

object SimpleDirectory {

  private val RequireDoesNotExist = Some((KeyValueUpdate.TimestampRequirement.DoesNotExist, HLCTimestamp.now))
  private val RequireExists = Some((KeyValueUpdate.TimestampRequirement.Exists, HLCTimestamp.now))

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
    val (initialInode, initialRevision) = inodeState

    ftl match {
      case Some(f) => f

      case None =>
        initialInode.ocontents match {
          case Some(tkvl) =>
            val rootMgr = new SimpleDirectoryTLRootManager(fs.system, pointer.pointer, tkvl)
            val f = Future.successful(new MutableTieredKeyValueList(rootMgr))
            ftl = Some(f)
            f

          case None =>

            val promise: Promise[MutableTieredKeyValueList] = Promise()

            ftl = Some(promise.future)

            val (tkvlAllocaterUUID, config) = fs.directoryTableConfig

            val tkvlaFactory = fs.system.typeRegistry.getTypeFactory[TieredKeyValueListNodeAllocaterFactory](tkvlAllocaterUUID).get

            val tkvlAllocater = tkvlaFactory.createNodeAllocater(fs.system, config)

            def createTree(refreshedInode: DirectoryInode,
                           refreshedRevision: ObjectRevision): Unit = {

              def alloc(allocater: ObjectAllocater) = fs.system.transact { implicit tx =>
                for {
                  root <- allocater.allocateKeyValueObject(pointer.pointer, revision, Nil)
                } yield {
                  val tkvl = new TieredKeyValueListRoot(0, LexicalKeyOrdering, root, tkvlAllocaterUUID, config)
                  val newInode = inode.setContentTree(Some(tkvl))
                  tx.note(s"SimpleDirectory - creating directory TKVL. Root node is ${root.uuid}")
                  tx.overwrite(pointer.pointer, revision, newInode.toDataBuffer)
                  (newInode, tx.txRevision, tkvl)
                }
              }

              val f = for {
                allocater <- tkvlAllocater.tierNodeAllocater(0)
                (newInode, newRevision, tkvl) <- alloc(allocater)
              } yield {
                val rootMgr = new SimpleDirectoryTLRootManager(fs.system, pointer.pointer, tkvl)
                setCachedInode(newInode, newRevision)
                promise.success(new MutableTieredKeyValueList(rootMgr))
              }

              f.failed.foreach { _ =>
                refresh().foreach { _ =>
                  val (refreshedInode, refreshedRevision) = inodeState

                  // Check to see if someone else beat us to it before retrying the create
                  refreshedInode.ocontents match {
                    case Some(tkvl) =>
                      val rootMgr = new SimpleDirectoryTLRootManager(fs.system, pointer.pointer, tkvl)
                      promise.success(new MutableTieredKeyValueList(rootMgr))

                    case None => createTree(refreshedInode, refreshedRevision)
                  }
                }
              }
            }

            createTree(initialInode, initialRevision)

            promise.future
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
      target <- fs.lookup(pointer)
      _ = if (incref) {
        tx.note(s"Increfing link count on $name:${pointer.uuid}")
        tx.setRefcount(pointer.pointer, kvos.refcount, kvos.refcount.increment())
        target.prepareHardLink()
      }
      _=tx.note(s"Inserting $name:${pointer.uuid} into directory ${this.pointer.uuid}")
      _ <- tl.preparePut(name, pointer.toArray, RequireDoesNotExist)
    } yield ()
  }
  
  def prepareRename(oldName: String, newName: String)(implicit tx: Transaction, ec: ExecutionContext): Future[Unit] = {
    for {
      tl <- tree
      _=tx.note(s"Renaming $oldName to $newName in directory ${this.pointer.uuid}")
      _ <- tl.prepareRename(oldName, newName, oldKeyRequirement = None, newKeyRequirement = RequireDoesNotExist)
    } yield ()
  }
  
  def hardLink(name: String, file: BaseFile)(implicit ec: ExecutionContext): Future[Unit] = {
    implicit val tx: Transaction = fs.system.newTransaction()
    
    prepareInsert(name, file.pointer).flatMap { _ =>
      file.prepareHardLink()
      tx.note(s"Hardlinking $name to file ${file.pointer.uuid} in directory ${pointer.uuid}")
      tx.commit().map(_=>())
    }
  }
  
  def prepareDelete(name: String, decref: Boolean=true)(implicit tx: Transaction, ec: ExecutionContext): Future[Future[Unit]] = {
    tx.note(s"Preparing delete of $name in directory ${pointer.uuid}")

    def del(tl: MutableTieredKeyValueList, oentry: Option[InodePointer]): Future[Future[Unit]] = oentry match {
      case None => Future.successful(Future.unit) // Directory entry not found. We're done!
      
      case Some(inodePtr) =>

        val fdelEntryPrep = tl.prepareDelete(name, RequireExists)
        
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
    tx.note(s"Preparing directory for deletion ${pointer.uuid}")

    def prep(rootNode: KeyValueObjectState): Future[Unit] = {
      
      if (rootNode.contents.nonEmpty)
        Future.failed(DirectoryNotEmpty(pointer))
      else {
        tx.lockRevision(rootNode.pointer, rootNode.revision)
        tx.setRefcount(rootNode.pointer, rootNode.refcount, rootNode.refcount.decrement())
        Future.unit
      }
    }

    inode.ocontents match {
      case None => Future.unit

      case Some(_) =>
        for {
          tl <- tree
          node <- tl.fetchMutableNode(Key.AbsoluteMinimum)
          _ <- prep(node.kvos)
        } yield ()
    }
  }
}