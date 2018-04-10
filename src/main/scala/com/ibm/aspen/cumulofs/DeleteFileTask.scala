package com.ibm.aspen.cumulofs

import com.ibm.aspen.base.task.SteppedDurableTask
import com.ibm.aspen.core.objects.keyvalue.Key
import scala.concurrent.ExecutionContext
import scala.concurrent.Future
import com.ibm.aspen.util._
import com.ibm.aspen.base.task.DurableTaskType
import java.util.UUID
import com.ibm.aspen.base.AspenSystem
import com.ibm.aspen.base.task.DurableTaskPointer
import com.ibm.aspen.core.objects.ObjectRevision
import com.ibm.aspen.core.objects.keyvalue.Value
import com.ibm.aspen.base.task.DurableTask
import com.ibm.aspen.base.Transaction
import sun.reflect.generics.reflectiveObjects.NotImplementedException

object DeleteFileTask {
  private val BaseKeyId = SteppedDurableTask.ReservedToKeyId
  
  private val FileSystemUUIDKey = Key(BaseKeyId + 1)
  private val InodePointerKey   = Key(BaseKeyId + 2)
  private val DataRootKey       = Key(BaseKeyId + 3)
  
  /** Configures the transaction to delete the target inode and launch a file deletion task
   *  that will remove it from the Inode table and preform any necessary cleanup activities
   *  such as deleting file data and their associated tiered lists.
   */
  def prepare(
      fs: FileSystem, 
      victim: InodePointer)(implicit tx: Transaction, ec: ExecutionContext): Future[Unit] = {
    
    fs.inodeLoader.load(victim) map { inode =>
  
      if (inode.refcount.count != 1)
        throw new Exception("Refcount is not one") 
        
      val common = List(
              (FileSystemUUIDKey, uuid2byte(fs.uuid)),
              (InodePointerKey,   victim.toArray)
             )
             
      val (fprep, taskState) = inode match {
        case d: DirectoryInode =>
          
          val content = d.contentTree match {
            case None => common
            case Some(root) => (DataRootKey, root.toArray()) :: common 
          }
          
          (fs.loadDirectory(d.pointer).prepareForDirectoryDeletion(), content)
        
        case _ => throw new NotImplementedException
      }
      
      tx.setRefcount(inode.pointer.pointer, inode.refcount, inode.refcount.decrement())
      
      for {
        _ <- fs.localTaskGroup.prepareTask(TaskType, taskState).map(_ => ())
        _ <- fprep
      } yield ()
    }
  }
  
  private[cumulofs] object TaskType extends DurableTaskType {
    
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
       extends SteppedDurableTask(pointer, system, revision, initialState) {
  
  import DeleteFileTask._
  
  def suspend(): Unit = {}
  
  def beginStep(): Unit = {
    
    // The task group executor is created by the FileSystem class so its guaranteed to have already
    // been registered.
    val fs = FileSystem.getRegisteredFileSystem(byte2uuid(state(FileSystemUUIDKey))).get
    
    val iptr = InodePointer(state(InodePointerKey))
    
    val fremovedFromTable = fs.inodeTable.delete(iptr)
    
    val fdataDeleted = iptr match {
      case _: DirectoryPointer => deleteDirectoryData(fs)
      case _: FilePointer => deleteFileData(fs)
      case _ => Future.unit
    }
    
    for {
      _ <- fremovedFromTable
      _ <- fdataDeleted
    } yield {
      system.transactUntilSuccessful { tx =>
        completeTask(tx)
        Future.unit
      }
    }
  }
  
  def deleteDirectoryData(fs: FileSystem): Future[Unit] = Future.unit 
  
  def deleteFileData(fs: FileSystem): Future[Unit] = Future.unit
}