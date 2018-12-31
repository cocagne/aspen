package com.ibm.aspen.amorfs.impl

import java.util.UUID

import com.ibm.aspen.base.task.{DurableTask, DurableTaskPointer, DurableTaskType, SteppedDurableTask}
import com.ibm.aspen.base.{AspenSystem, Transaction}
import com.ibm.aspen.core.objects.keyvalue.{Key, Value}
import com.ibm.aspen.core.objects.{DataObjectState, ObjectRevision}
import com.ibm.aspen.amorfs._
import com.ibm.aspen.util._
import org.apache.logging.log4j.scala.Logging

import scala.concurrent.{ExecutionContext, Future}

object DeleteFileTask {
  private val BaseKeyId = SteppedDurableTask.ReservedToKeyId
  
  private val FileSystemUUIDKey = Key(BaseKeyId + 1)
  private val InodePointerKey   = Key(BaseKeyId + 2)

  /** Configures the transaction to delete the target inode and launch a file deletion task
    *  that will remove it from the Inode table and preform any necessary cleanup activities
    *  such as deleting file data and their associated tiered lists.
    *
    *  Note that this method does NOT check the link count. This should only be called from
    *  the files prepareUnlink method.
    */
  def prepareFileDeletion(
      fs: FileSystem,
      victim: BaseFile)(implicit tx: Transaction, ec: ExecutionContext): Future[Future[Unit]] = {
    tx.note(s"Creating DeleteFileTask for victim inode: ${victim.pointer.uuid}")

    val inode = victim.inode
    val revision = victim.revision

    val content = List((FileSystemUUIDKey, uuid2byte(fs.uuid)), (InodePointerKey, victim.pointer.toArray))

    val ftask = fs.localTaskGroup.prepareTask(TaskType, content)

    inode match {
      case d: DirectoryInode =>
        for {
          _ <- fs.loadDirectory(victim.pointer.asInstanceOf[DirectoryPointer], d, revision).prepareForDirectoryDeletion()
          taskReady <- ftask
        } yield taskReady.map(_=>())

      case _: FileInode => for {
        taskReady <- ftask
      } yield taskReady.map(_=>())

      case _ => Future.successful(Future.unit) // No additional work to do
    }
  }
  
  private[amorfs] object TaskType extends DurableTaskType {
    
    val typeUUID: UUID = UUID.fromString("f38b1e27-14c9-459a-9da0-793334445f01")
   
    def createTask(
        system: AspenSystem, 
        pointer: DurableTaskPointer, 
        revision: ObjectRevision, 
        state: Map[Key, Value])(implicit ec: ExecutionContext): DurableTask = new DeleteFileTask(system, pointer, revision, state)
  }
}

class DeleteFileTask private (
    system: AspenSystem,
    pointer: DurableTaskPointer, 
    revision: ObjectRevision, 
    initialState: Map[Key, Value])(implicit ec: ExecutionContext)
       extends SteppedDurableTask(pointer, system, revision, initialState) with Logging {
  
  import DeleteFileTask._
  
  def suspend(): Unit = {}
  
  def beginStep(): Unit = {
    
    // The task group executor is created by the FileSystem class so its guaranteed to have already
    // been registered.
    val fs = FileSystem.getRegisteredFileSystem(byte2uuid(state(FileSystemUUIDKey))).get
    
    val iptr = InodePointer(state(InodePointerKey))

    logger.info(s"DeleteFileTask - Deleting file ${iptr.number} : ${iptr.uuid}")
    
    def destroyInode(dos: DataObjectState): Future[Unit] = {
      implicit val tx: Transaction = system.newTransaction()
      tx.note(s"DeleteFileTask - Setting refcount of ${iptr.uuid} to zero")
      tx.setRefcount(iptr.pointer, dos.refcount, dos.refcount.setCount(0))
      tx.commit().map(_ =>())
    }
    
    for {
      file <- fs.lookup(iptr)
      _ <- file.freeResources()
      dos <- system.readObject(iptr.pointer)
      _ <- destroyInode(dos)
      _ <- fs.inodeTable.delete(iptr)
    } yield {
      system.transactUntilSuccessful { tx =>
        tx.note("DeleteFileTask - marking task complete")
        completeTask(tx)

        tx.result.foreach { _ =>
          logger.info(s"DeleteFileTask - Finished deleting file ${iptr.number} : ${iptr.uuid}")
        }

        Future.unit
      }
    }
  }

}