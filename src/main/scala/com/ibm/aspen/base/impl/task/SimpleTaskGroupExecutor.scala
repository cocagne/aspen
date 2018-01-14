package com.ibm.aspen.base.impl.task

import com.ibm.aspen.base.TaskGroupExecutor
import com.ibm.aspen.base.ObjectAllocater
import com.ibm.aspen.base.RetryStrategy
import com.ibm.aspen.base.TaskTypeRegistry
import com.ibm.aspen.core.objects.ObjectPointer
import java.util.UUID
import com.ibm.aspen.base.AspenSystem
import scala.concurrent.Future
import scala.concurrent.ExecutionContext
import com.ibm.aspen.base.Task
import com.ibm.aspen.core.objects.ObjectRevision
import scala.concurrent.duration._
import scala.util.Failure
import scala.util.Success
import com.ibm.aspen.core.objects.ObjectRefcount
import com.ibm.aspen.base.impl.BackgroundTask
import com.ibm.aspen.base.TaskGroupType
import scala.concurrent.Promise
import com.ibm.aspen.core.objects.DataObjectPointer

object SimpleTaskGroupExecutor {
  val PollingPeriod = Duration(30, SECONDS)
  val NumConcurrentTasks = 5
  
  private[this] var executors = Map[UUID, SimpleTaskGroupExecutor]()
  
  private def registerExecutor(e: SimpleTaskGroupExecutor) = synchronized { executors += (e.taskGroupInstanceUUID -> e) }
  
  private[task] def notifyTasksAdded(taskGroupInstanceUUID: UUID): Unit = synchronized { 
    executors.get(taskGroupInstanceUUID).foreach( stge => stge.loadState() ) 
  }
}

/** Very simple & inefficient TaskGroupExecutor
 * 
 */
class SimpleTaskGroupExecutor(
    val system: AspenSystem, 
    val taskGroupInstanceUUID: UUID, 
    val taskGroupDefinitionPointer: DataObjectPointer, 
    val taskRegistry: TaskTypeRegistry, 
    val retryStrategy: RetryStrategy, 
    val taskObjectAllocater: ObjectAllocater)(implicit ec: ExecutionContext) extends TaskGroupExecutor {
  
  private[this] var tasks: List[TaskDefinition] = Nil
  private[this] var groupDefinitionRevision = ObjectRevision(new UUID(0,0))
  private[this] var activeTasks = Map[UUID, Future[Task]]()
  private[this] var pendingDeletes = Set[UUID]()
  private[this] var isPruning = false
  private[this] var isLoading = false
  private[this] var shuttingDown: Option[Future[Unit]] = None
  private[this] var initializing = Promise[Unit]()
  
  val taskGroupType: TaskGroupType = SimpleTaskGroupType
  
  import SimpleTaskGroupExecutor._
  
  SimpleTaskGroupExecutor.registerExecutor(this)
    
  val statePollingTask = BackgroundTask.schedulePeriodic(period=PollingPeriod, callNow=true) {
    loadState()
  }
  
  val initialized: Future[Unit] = initializing.future
  
  def shutdown(): Future[Unit] = synchronized {
    shuttingDown match {
      case Some(f) => f
      
      case None =>
        statePollingTask.cancel()
        
        val f = Future.sequence(activeTasks.values.map(ftask => ftask.map(t => t.pause()))).map(_ => ())
        
        shuttingDown = Some(f)
        
        f
    }
  }
  
  private def loadState(): Unit = synchronized {
    
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
          if (!pendingDeletes.isEmpty)
            pruneDeletedTasks()
          startTasks()
          if (!initializing.isCompleted)
            initializing.success(())
        }
      }
    }
  }
  
  private[this] def startTasks(): Unit = synchronized {
    if (!tasks.isEmpty) {
      def rlaunch(t: List[TaskDefinition]): Unit = {
        if (!t.isEmpty && activeTasks.size < NumConcurrentTasks) {
          if (!activeTasks.contains(t.head.taskUUID) && !pendingDeletes.contains(t.head.taskUUID)) 
            startTask(t.head)
          rlaunch(t.tail)
        }
      }
      rlaunch(tasks)
    }
  }
  
  // Must be called from within a synchronized block
  private[this] def startTask(td: TaskDefinition): Unit = {
    
    taskRegistry.getTaskType(td.taskTypeUUID) foreach { tt => 
  
      val ftask = tt.createTaskExecutor(system, td.taskUUID, td.taskObject)
      
      activeTasks += (td.taskUUID -> ftask)
      
      ftask onComplete {
        case Failure(reason) => 
          synchronized { activeTasks -= td.taskUUID }
          // Next polling period will try to re-start it. Simple mechanism for overcomming network disconnects
          // has the potential to forever lock-up the executor if a real error is present though
          
          
        case Success(task) => 
          task.resume()
          task.complete onComplete {
            case Failure(reason) => // TODO: Log failure. These should not happen
              deleteTask(td.taskUUID)
              
            case Success(_) =>
              deleteTask(td.taskUUID)
          }
      }
    }
  }

  private[this] def deleteTask(taskUUID: UUID): Unit = synchronized {
    activeTasks -= taskUUID
    pendingDeletes += taskUUID
    
    pruneDeletedTasks()
    startTasks()
  }
  
  private[this] def pruneDeletedTasks(): Unit =  {
    
    val (requiredRevision, currentTasks, completedTasks, wasPruning) = synchronized {
      val p = isPruning
      if (!isPruning && !pendingDeletes.isEmpty)
        isPruning = true
      (groupDefinitionRevision, tasks, pendingDeletes, p) 
    }
    
    if (!wasPruning && !completedTasks.isEmpty) {
      implicit val tx = system.newTransaction()
      
      val prunedTasks = currentTasks.foldLeft(List[TaskDefinition]()) { (l, td) =>
        if (completedTasks.contains(td.taskUUID)) {
          tx.setRefcount(td.taskObject, ObjectRefcount(0,1), ObjectRefcount(1,0)) // Delete object by setting it's refcount to zero
          l
        }
        else
          td :: l
      }
      
      val newRevision = tx.overwrite(taskGroupDefinitionPointer, requiredRevision, TaskCodec.encodeTaskGroupDefinition(prunedTasks))
          
      tx.commit() onComplete {
        
        case Failure(reason) => synchronized {
          isPruning = false
          loadState() // Tasks were likely added. Refresh to get new object revision
        }
        
        case Success(_) => synchronized {
          tasks = prunedTasks
          groupDefinitionRevision = newRevision
          pendingDeletes = pendingDeletes -- completedTasks
          isPruning = false
          
          if (!pendingDeletes.isEmpty)
            pruneDeletedTasks()
        }
      }
    }
  }
}