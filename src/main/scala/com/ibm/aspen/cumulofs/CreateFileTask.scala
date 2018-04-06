package com.ibm.aspen.cumulofs

import com.ibm.aspen.base.task.DurableTaskType
import java.util.UUID
import com.ibm.aspen.base.AspenSystem
import com.ibm.aspen.base.task.DurableTaskPointer
import com.ibm.aspen.core.objects.ObjectRevision
import com.ibm.aspen.core.objects.keyvalue.Key
import com.ibm.aspen.core.objects.keyvalue.Value
import scala.concurrent.ExecutionContext
import com.ibm.aspen.base.task.DurableTask
import com.ibm.aspen.base.task.SteppedDurableTask
import com.ibm.aspen.base.task.LocalTaskGroup
import com.ibm.aspen.base.Transaction
import scala.concurrent.Future
import com.ibm.aspen.core.objects.keyvalue.KeyValueOperation

object CreateFileTask extends {
  
  private val BaseKeyId = SteppedDurableTask.ReservedToKeyId
  
  private val DirectoryKey    = Key(BaseKeyId + 1)
  private val NameKey         = Key(BaseKeyId + 2)
  private val InodePointerKey = Key(BaseKeyId + 3)
  
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
   */
  def execute(
      fs: FileSystem, 
      directory: DirectoryPointer, 
      name: String, 
      ftype: FileType.Value,
      inodeOps: List[KeyValueOperation])(implicit ec: ExecutionContext): Future[InodePointer] = {
    
    val encodedDir = directory.toArray
    val encodedName = string2arr(name)

    val ftaskPrepared = fs.system.retryStrategy.retryUntilSuccessful {
      
      implicit val tx = fs.system.newTransaction()
      
      val fcommit = for {
        newInode <- fs.inodeTable.prepareInodeAllocation(ftype, inodeOps)
        
        taskState = List(
            (DirectoryKey,    encodedDir),
            (NameKey,         encodedName),
            (InodePointerKey, newInode.toArray))
            
        taskPrepared <- fs.localTaskGroup.prepareTask(TaskType, taskState)
        
        committed <- tx.commit()
        
      } yield taskPrepared.map(_ => newInode)
      
      fcommit.failed.foreach(reason => tx.invalidateTransaction(reason))
      
      fcommit
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
  
  
  def suspend(): Unit = {}
  
  
  def beginStep(): Unit = ???
}