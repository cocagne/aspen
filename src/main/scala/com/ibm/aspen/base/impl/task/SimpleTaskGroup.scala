package com.ibm.aspen.base.impl.task

import java.util.UUID
import com.ibm.aspen.base.RetryStrategy
import com.ibm.aspen.core.objects.ObjectPointer
import scala.concurrent.ExecutionContext
import scala.concurrent.Future
import com.ibm.aspen.base.AspenSystem
import com.ibm.aspen.base.ObjectAllocater
import com.ibm.aspen.core.objects.ObjectRevision
import scala.concurrent.Promise
import scala.util.Success
import scala.util.Failure
import com.ibm.aspen.base.TaskGroup
import com.ibm.aspen.base.TaskType
import com.ibm.aspen.core.DataBuffer
import scala.annotation.tailrec
import scala.collection.immutable.Queue
import com.ibm.aspen.core.objects.ObjectRefcount
import com.ibm.aspen.base.Task
import com.ibm.aspen.base.TaskGroupExecutor
import com.ibm.aspen.base.TaskGroupType
import com.ibm.aspen.core.objects.DataObjectPointer


class SimpleTaskGroup(
    val system: AspenSystem,
    val taskGroupInstanceUUID: UUID,
    val taskGroupDefinitionPointer: DataObjectPointer)(implicit ec: ExecutionContext) extends TaskGroup {
  
  val groupTypeUUID: UUID  = SimpleTaskGroupType.typeUUID
  val taskGroupType: TaskGroupType = SimpleTaskGroupType
  
  case class PendingCreate(taskTypeUUID:UUID, taskUUID: UUID, 
                           taskObject: DataObjectPointer, taskRevision: ObjectRevision, promise:Promise[Unit])
  
  private[this] var tasks: List[TaskDefinition] = Nil
  private[this] var groupDefinitionRevision = ObjectRevision(new UUID(0,0))
  private[this] var pending: List[PendingCreate] = Nil
  private[this] var isUpdating = false
  private[this] var isLoading = false
  
  def isEmpty: Boolean = synchronized { tasks.isEmpty }
  
  // Fetch initial group state on object creation
  loadState()
  
  private[this] def loadState(): Unit = synchronized {
    if (!isLoading) {
      isLoading = true
      SimpleTaskGroupType.loadGroupState(system, taskGroupDefinitionPointer) onComplete {
        case Failure(_) => synchronized { 
          isLoading = false 
        }
        
        case Success(tgs) => synchronized { 
          isLoading = false 
          groupDefinitionRevision = tgs.revision
          tasks = tgs.tasks
          if (!pending.isEmpty)
            updateGroup()
        }
      }
    }
  }
  
  override def addTask(
      taskTypeUUID: UUID, 
      taskUUID: UUID, 
      taskDefinitionPointer: DataObjectPointer, 
      requiredRevision: ObjectRevision): Future[Unit] = {
    
    val pc = PendingCreate(taskTypeUUID, taskUUID, taskDefinitionPointer, requiredRevision, Promise[Unit]())
    
    synchronized {
      pending = pc :: pending
      updateGroup()
    }
    
    pc.promise.future
  }
  
  private[this] def updateGroup(): Unit =  {
    
    val (requiredRevision, currentTasks, pendingTasks, wasUpdating) = synchronized {
      val updating = isUpdating
      if (!isUpdating && !pending.isEmpty)
        isUpdating = true
      (groupDefinitionRevision, tasks, pending, updating) 
    }
    
    if (!wasUpdating && !pendingTasks.isEmpty) {
      
      // Validate the version number of all tasks in the pending list and complete the futures for those tasks that have changed
      // revisions
      //
      val fValidPendingTasks = Future.sequence(pendingTasks.map(pc => system.readObject(pc.taskObject).map(osd => (pc, osd)))).map { lst =>
        lst.filter { t =>
          val (pc, osd) = t
          
          if (pc.taskRevision != osd.revision) {
            // Revision has changed, go ahead and complete the futures
            pc.promise.success(())
            false
            
          } else
            true
        } 
      }.map(lst => lst.map(t => t._1))
          
      fValidPendingTasks onComplete {
        case Failure(reason) => 
          synchronized { isUpdating = false }
          updateGroup() // try again
          
        case Success(validatedTasks) =>
          implicit val tx = system.newTransaction()
          
          validatedTasks.foreach { pc => 
            tx.bumpVersion(pc.taskObject, pc.taskRevision)
          }
          
          val newTasks = tasks ++ validatedTasks.map(pc => TaskDefinition(pc.taskTypeUUID, pc.taskUUID, pc.taskObject)).reverse
          
          val newRevision = tx.overwrite(taskGroupDefinitionPointer, requiredRevision, TaskCodec.encodeTaskGroupDefinition(newTasks))
          
          tx.commit() onComplete {
        
            case Failure(reason) => synchronized {
              isUpdating = false
              loadState() // Tasks were likely added. Refresh to get new object revision
            }
            
            case Success(_) => synchronized {
              isUpdating = false
              tasks = newTasks
              groupDefinitionRevision = newRevision
              val added = validatedTasks.map(pc => pc.taskUUID).toSet
              pending = pending.filter(pc => !added.contains(pc.taskUUID))
              
              validatedTasks.foreach { pc => pc.promise.success(()) }
              
              if (!pending.isEmpty)
                updateGroup()
              
              // If the executor happens to be in the same JVM, directly inform it of the new tasks
              SimpleTaskGroupExecutor.notifyTasksAdded(taskGroupInstanceUUID)
            }
          }
      }
    }
  }
  
  
}