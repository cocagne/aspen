package com.ibm.aspen.cumulofs.impl

import java.util.UUID

import com.ibm.aspen.base.AspenSystem
import com.ibm.aspen.base.task.{DurableTask, DurableTaskPointer, DurableTaskType, SteppedDurableTask}
import com.ibm.aspen.core.objects.ObjectRevision
import com.ibm.aspen.core.objects.keyvalue.{Key, Value}
import com.ibm.aspen.cumulofs.{arr2string => _, _}
import com.ibm.aspen.util._

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
   *   TODO: Handle "Out of Space" allocation errors
   */
  def execute(
      fs: FileSystem, 
      directory: DirectoryPointer, 
      name: String,
      inode: Inode)(implicit ec: ExecutionContext): Future[InodePointer] = {
    
    val encodedDir = directory.toArray
    val encodedName = string2arr(name)
    val encodedFS = uuid2byte(fs.uuid)

    val ftaskPrepared = fs.system.transactUntilSuccessful { implicit tx =>
      
      for {
        newInode <- fs.inodeTable.prepareInodeAllocation(inode)
          
        taskState = List(
            (DirectoryKey,      encodedDir),
            (NameKey,           encodedName),
            (InodePointerKey,   newInode.toArray),
            (FileSystemUUIDKey, encodedFS))

        taskPrepared <- fs.localTaskGroup.prepareTask(TaskType, taskState)
        
        _ <- tx.commit()
        
      } yield {
        taskPrepared.map(_ => newInode)
      }
    }
    
    ftaskPrepared.flatMap( ftaskComplete => ftaskComplete )
  }
  
  private[cumulofs] object TaskType extends DurableTaskType {
    
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
       extends SteppedDurableTask(pointer, system, revision, initialState) {
  
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
    
    fs.system.transactUntilSuccessful { implicit tx =>

      for {

        dir <- fs.loadDirectory(directoryPointer)

        _ <- dir.prepareInsert(name, newInode)

        _ = completeTask(tx)
        
      } yield ()
    }
  
  }
}