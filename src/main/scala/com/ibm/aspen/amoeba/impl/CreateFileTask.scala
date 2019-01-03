package com.ibm.aspen.amoeba.impl

import java.util.UUID

import com.ibm.aspen.base.{AspenSystem, StopRetrying, Transaction}
import com.ibm.aspen.base.task.{DurableTask, DurableTaskPointer, DurableTaskType, SteppedDurableTask}
import com.ibm.aspen.core.objects.ObjectRevision
import com.ibm.aspen.core.objects.keyvalue.{Key, Value}
import com.ibm.aspen.core.read.FatalReadError
import com.ibm.aspen.amoeba._
import com.ibm.aspen.amoeba.error.{AmoebaError, DirectoryEntryExists, InvalidInode}
import com.ibm.aspen.util._
import org.apache.logging.log4j.scala.Logging

import scala.concurrent.{ExecutionContext, Future}

object CreateFileTask {
  
  private val BaseKeyId = SteppedDurableTask.ReservedToKeyId
  
  private val DirectoryKey      = Key(BaseKeyId + 1)
  private val NameKey           = Key(BaseKeyId + 2)
  private val InodePointerKey   = Key(BaseKeyId + 3)
  private val FileSystemUUIDKey = Key(BaseKeyId + 4)
  
  /** Creates a file and inserts it into the specified directory
    *
    *   There are three logical steps involved:
    *      1. Allocation of the Inode's Key-Value object
    *      2. Insertion into the InodeTable, which also assigns the Inode number
    *      3. Insertion of the InodePointer (derived from steps 1 and 2) into the Directory's TieredList
    *
    *   If the client were to crash between steps 2 & 3, the allocated inode would be forever lost. To prevent this
    *   from happening, file creation is implemented in terms of a DurableTask to ensure that the even if a crash occurs,
    *   the full sequnce of steps will eventually complete.
    *
    *   The first two steps and the creation of the durable task are all done within the same transaction. In the case of
    *   contention on the Inode table, we simply retry until the operation is successful. Similarly, successful insertion into
    *   the direcotry tree is done atomically with deletion of the task. If the transaction performing this step fails, it is
    *   simply retried until it is successful.
    *
    *   Outter future completes when transaction is ready for commit. Inner when transaction successfully completes
    *
    *   TODO: Handle "Out of Space" allocation errors
   */
  def prepareFileCreation(
      fs: FileSystem, 
      directory: DirectoryPointer, 
      name: String,
      inode: Inode)(implicit tx: Transaction, ec: ExecutionContext): Future[Future[InodePointer]] = {

    val encodedDir = directory.toArray
    val encodedName = string2arr(name)
    val encodedFS = uuid2byte(fs.uuid)

    for {
      newInode <- fs.inodeTable.prepareInodeAllocation(inode)

      _=tx.note(s"Creating Task: CreateFileTask for $name:${newInode.uuid} in directory ${directory.uuid}")

      taskState = List(
          (DirectoryKey,      encodedDir),
          (NameKey,           encodedName),
          (InodePointerKey,   newInode.toArray),
          (FileSystemUUIDKey, encodedFS))

      ftaskResult <- fs.localTaskGroup.prepareTask(TaskType, taskState)

    } yield {
      ftaskResult.map(_ => newInode)
    }
  }
  
  private[amoeba] object TaskType extends DurableTaskType {
    
    val typeUUID: UUID = UUID.fromString("6af101d1-e40b-478f-a820-aab934e71ece")
   
    def createTask(
        system: AspenSystem, 
        pointer: DurableTaskPointer, 
        revision: ObjectRevision, 
        state: Map[Key, Value])(implicit ec: ExecutionContext): DurableTask = new CreateFileTask(system, pointer, revision, state)
  }
}

class CreateFileTask private (
    system: AspenSystem,
    pointer: DurableTaskPointer, 
    revision: ObjectRevision, 
    initialState: Map[Key, Value])(implicit ec: ExecutionContext)
       extends SteppedDurableTask(pointer, system, revision, initialState) with Logging {
  
  import CreateFileTask._
  
  def suspend(): Unit = {}
  
  def beginStep(): Unit = {
    val directoryPointer = InodePointer(state(DirectoryKey)).asInstanceOf[DirectoryPointer]
    val name = arr2string(state(NameKey))
    val newInode = InodePointer(state(InodePointerKey))
    val fsUUID = byte2uuid(state(FileSystemUUIDKey))

    // The task group executor is created by the FileSystem class so its guaranteed to have already
    // been registered.
    val fs = FileSystem.getRegisteredFileSystem(fsUUID).get

    logger.info(s"CreateFileTask - creating file $name in directory ${directoryPointer.uuid}")

    def onFatalError(err: AmoebaError): Future[Unit] = {
      logger.info(s"CreateFileTask - fatal error encountered while creating $name in directory ${directoryPointer.uuid}. Abandoning task")
      system.transactUntilSuccessful { implicit tx =>
        tx.note("CreateFileTask - Abandoning file creation due to fatal error")
        failTask(tx, err)
        fs.lookup(newInode).flatMap(file => DeleteFileTask.prepareFileDeletion(fs, file).map(_ => ()))
      } map { _ =>
        throw StopRetrying(err)
      }
    }

    def onFail(err: Throwable): Future[Unit] = err match {
      case _: FatalReadError => onFatalError(InvalidInode(directoryPointer.number))
      case _ => for {
        dir <- fs.loadDirectory(directoryPointer)
        e <- dir.getEntry(name)
        _ <- if (e.nonEmpty) {
          logger.info(s"CreateFileTask - $name already exists in directory ${directoryPointer.uuid}. Abandoning file creation")
          onFatalError(DirectoryEntryExists(directoryPointer, name))
        } else Future.unit
      } yield ()
    }
    
    fs.system.transactUntilSuccessfulWithRecovery(onFail) { implicit tx =>
      tx.note(s"CreateFileTask - insert new file $name:${newInode.uuid} into directory ${directoryPointer.uuid}")

      tx.result.foreach { _ =>
        logger.info(s"CreateFileTask - finished creating new file $name:${newInode.uuid} in directory ${directoryPointer.uuid}")
      }

      for {

        dir <- fs.loadDirectory(directoryPointer)

        // Initial file creation already sets the refcount to 1. Skip incref for this insertion
        _ <- dir.prepareInsert(name, newInode, incref = false)

        _ = completeTask(tx)
        
      } yield ()
    }
  }
}