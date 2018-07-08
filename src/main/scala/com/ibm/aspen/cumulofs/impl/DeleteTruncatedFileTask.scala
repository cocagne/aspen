package com.ibm.aspen.cumulofs.impl

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
import com.ibm.aspen.base.tieredlist.TieredKeyValueList
import com.ibm.aspen.core.objects.keyvalue.LexicalKeyOrdering
import com.ibm.aspen.base.tieredlist.SimpleMutableTieredKeyValueList
import com.ibm.aspen.core.objects.ObjectRefcount
import com.ibm.aspen.core.objects.KeyValueObjectState
import com.ibm.aspen.cumulofs.FileSystem
import com.ibm.aspen.core.objects.DataObjectPointer

object DeleteTruncatedFileTask {
  private val BaseKeyId = SteppedDurableTask.ReservedToKeyId
  
  private val RootPointerKey    = Key(BaseKeyId + 1)
  private val TierKey           = Key(BaseKeyId + 2)

  /** Configures the transaction to create an launch a task that will recursively delete
   *  the truncated index and data segments
   */
  def prepare(
      fs: FileSystem,
      tier: Int,
      root: DataObjectPointer)(implicit tx: Transaction, ec: ExecutionContext): Future[Future[Unit]] = {
       
     fs.localTaskGroup.prepareTask(TaskType, List(
         (RootPointerKey, root.toArray),
         (TierKey, int2byte(tier)))).map { _ => Future.unit }
    
  }
  
  private[cumulofs] object TaskType extends DurableTaskType {
    
    val typeUUID: UUID = UUID.fromString("2310c3f1-d448-4b63-a969-0ef6998b7410")
   
    def createTask(
        system: AspenSystem, 
        pointer: DurableTaskPointer, 
        revision: ObjectRevision, 
        state: Map[Key, Value])(implicit ec: ExecutionContext): DurableTask = new DeleteTruncatedFileTask(system, pointer, revision, state)
  }
}

class DeleteTruncatedFileTask private (
    system: AspenSystem,
    pointer: DurableTaskPointer, 
    revision: ObjectRevision, 
    initialState: Map[Key, Value])(implicit ec: ExecutionContext)
       extends SteppedDurableTask(pointer, system, revision, initialState) {
  
  import DeleteTruncatedFileTask._
  
  def suspend(): Unit = {}
  
  def beginStep(): Unit = {

    val root = DataObjectPointer(state(RootPointerKey))
    
    val tier = byte2int(state(TierKey))
    
    println(s"******* STARTING FILE TRUNCATION TASK *****")
    
    FileIndex.destroyTruncatedTree(system, tier, root).foreach { _ =>
      system.transactUntilSuccessful { tx =>
        println(s"******* COMPLETED FILE TRUNCATION TASK *****")
        completeTask(tx)
        Future.unit
      }
    }
  }

}