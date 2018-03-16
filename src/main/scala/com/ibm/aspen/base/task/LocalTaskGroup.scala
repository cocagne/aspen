package com.ibm.aspen.base.task

import com.ibm.aspen.core.objects.keyvalue.Key
import java.util.UUID
import scala.concurrent.ExecutionContext
import scala.concurrent.Future
import com.ibm.aspen.core.objects.KeyValueObjectPointer
import com.ibm.aspen.core.objects.ObjectRevision
import com.ibm.aspen.core.objects.keyvalue.Insert
import com.ibm.aspen.util._
import com.ibm.aspen.base.AspenSystem
import com.ibm.aspen.base.ObjectAllocater
import com.ibm.aspen.base.Transaction
import com.ibm.aspen.core.objects.KeyValueObjectState
import com.ibm.aspen.base.tieredlist.SimpleMutableTieredKeyValueList
import com.ibm.aspen.base.tieredlist.TieredKeyValueList
import com.ibm.aspen.core.objects.keyvalue.Value
import java.nio.ByteBuffer
import scala.concurrent.Promise
import scala.collection.immutable.Queue
import com.ibm.aspen.core.objects.keyvalue.IntegerKeyOrdering
import scala.util.Failure
import scala.util.Success

object LocalTaskGroup extends TaskGroupType {
  val AllocaterKey = Key(Array[Byte](1))
  val TaskListKey = Key(Array[Byte](2))
  
  val TaskListNodeSize = 16 * 1024 // TODO: Make this configurable?
  
  val typeUUID = UUID.fromString("f3051f22-9052-4c71-a469-22047b9edd8b")
  /*
  def encodeInt(i: Int): Array[Byte] = {
    val arr = new Array[Byte](4)
    ByteBuffer.wrap(arr).putInt(i)
    arr
  }
  def decodeInt(arr: Array[Byte]): Int = ByteBuffer.wrap(arr).getInt()
  */
  sealed abstract class ReusableTask {
    val taskNumber: Int
    val taskPointer: DurableTaskPointer
  }
  
  case class IdleTask(taskNumber: Int, taskPointer: DurableTaskPointer, revision: ObjectRevision) extends ReusableTask
  case class ActiveTask(taskNumber: Int, taskPointer: DurableTaskPointer, task: DurableTask) extends ReusableTask
  
  
  def initializeNewGroup(
      system: AspenSystem,
      groupPointer: TaskGroupPointer, 
      revision: ObjectRevision, 
      objectAllocaterUUID: UUID)(implicit ec: ExecutionContext): Future[LocalTaskGroup] = {
    
    implicit val tx = system.newTransaction()
    
    val ts = tx.timestamp()
    
    val content = List(Insert(TaskGroupType.GroupTypeKey, uuid2byte(typeUUID), ts), Insert(AllocaterKey, uuid2byte(objectAllocaterUUID), ts))
    
    for {
      allocater <- system.getObjectAllocater(objectAllocaterUUID)
      taskListRoot <- allocater.allocateKeyValueObject(groupPointer.kvPointer, revision, Nil, None)
      tlroot = TieredKeyValueList.Root(0, Array(objectAllocaterUUID), Array(TaskListNodeSize), taskListRoot)
      fullContent = Insert(TaskListKey, tlroot.toArray, ts) :: content
      _ = tx.overwrite(groupPointer.kvPointer, revision, Nil, fullContent)
      done <- tx.commit()
    } yield {
      new LocalTaskGroup(system, groupPointer, allocater, 
          new SimpleMutableTieredKeyValueList(system, Left(groupPointer.kvPointer), TaskListKey, IntegerKeyOrdering), 
          List())
    }
  }
  
  def createGroup(system:AspenSystem, groupState: KeyValueObjectState)(implicit ec: ExecutionContext): Future[LocalTaskGroup] = {

    val objectAllocaterUUID = byte2uuid(groupState.contents(AllocaterKey).value)
    val taskTree = new SimpleMutableTieredKeyValueList(system, Left(groupState.pointer), TaskListKey, IntegerKeyOrdering)
    
    var loadingTasks: List[Future[ReusableTask]] = Nil
    
    def taskVisitor(v: Value): Unit = {
      val taskNumber = v.key.intValue
      val pointer = KeyValueObjectPointer(v.value)
      val ftask = system.readObject(pointer) map { taskState =>
        
        val taskType = taskState.contents.get(DurableTask.TaskTypeKey) match {
          case None => DurableTask.IdleTaskType
          case Some(v) => byte2uuid(v.value)
        }
        
        if (taskType == DurableTask.IdleTaskType)
          IdleTask(taskNumber, DurableTaskPointer(pointer), taskState.revision)
        else {
          system.getTaskType(taskType) match {
            case None => throw new Exception("Unknown Task Type") // TODO need a better error handling strategy here
            case Some(ttype) => 
              val task = ttype.createTask(system, DurableTaskPointer(taskState.pointer), taskState.revision, taskState.contents) 
              ActiveTask(taskNumber, DurableTaskPointer(taskState.pointer), task) 
          }
        }
      }
      
      loadingTasks = ftask :: loadingTasks
    }
    
    val fallocater = system.getObjectAllocater(objectAllocaterUUID)
    val fvisit = taskTree.visitAll(taskVisitor)
    
    for {
      allocater <- fallocater
      _ <- fvisit
      taskList <- Future.sequence(loadingTasks)
    } yield {
      new LocalTaskGroup(system, TaskGroupPointer(groupState.pointer), allocater, taskTree, taskList)
    }
  }
}

class LocalTaskGroup(
    val system: AspenSystem,
    val pointer: TaskGroupPointer,
    protected val allocater: ObjectAllocater,
    protected val taskTree: SimpleMutableTieredKeyValueList,
    initialTasks: List[LocalTaskGroup.ReusableTask])(implicit ec: ExecutionContext) extends TaskGroupExecutor {
    
  val taskGroupType = LocalTaskGroup
  
  import LocalTaskGroup._
  
  protected var maxTaskNum: Int = initialTasks.foldLeft(0)( (max, rt) => if (rt.taskNumber > max) rt.taskNumber else max )
  
  protected var idleTasks = List[IdleTask]()
  protected var activeTasks = Map[Int, ActiveTask]()  
  protected val idleTaskAllocater = new IdleTaskAllocater()
  
  initialTasks.foreach { rt => rt match {
    case t: IdleTask => idleTasks = t :: idleTasks
    case t: ActiveTask => activeTasks += (t.taskNumber -> t)
  }}
  
  // Resume all active tasks
  activeTasks.foreach(t => executeTask(t._2))
  
  protected class IdleTaskAllocater {
    private var pending = Queue[(Int, Promise[IdleTask])]()
    private var allocating = false
    
    def allocateNext(): Unit = {
      val ((taskNumber, promise), newPending) = pending.dequeue
      pending = newPending
     
      val key = Key(taskNumber)
      
      val f = system.retryStrategy.retryUntilSuccessful {
        implicit val tx = system.newTransaction()
        
        val falloc = for {
          mnode <- taskTree.fetchMutableNode(taskNumber)
          newObject <- allocater.allocateKeyValueObject(mnode.kvos.pointer, mnode.kvos.revision, Nil, None)
          txPrepped <- mnode.prepreUpdateTransaction(List((Key(taskNumber), newObject.toArray)), Nil, Nil)
          done <- tx.commit()
        } yield (newObject, tx.txRevision)
        
        falloc.onComplete {
          case Failure(reason) => tx.invalidateTransaction(reason)
          case _ =>
        }
        
        falloc
      }
      
      f.foreach { t => synchronized { 
        promise.success(new IdleTask(taskNumber, DurableTaskPointer(t._1), t._2))
        allocating = false
        if (!pending.isEmpty)
          allocateNext()
      }}
    }
    
    def allocate(taskNumber: Int): Future[IdleTask] = synchronized {
      val p = Promise[IdleTask]()
      pending = pending.enqueue((taskNumber, p))
      
      if (!allocating)
        allocateNext()
        
      p.future
    }
  }
  
  protected def executeTask(at: ActiveTask): Unit = {
    activeTasks += (at.taskNumber -> at)
    at.task.resume()
    at.task.completed foreach { objectRevision => synchronized {
      activeTasks -= at.taskNumber
      idleTasks = IdleTask(at.taskNumber, at.taskPointer, objectRevision) :: idleTasks
    }}
  }
  
  def shutdown(): Future[Unit] = Future.unit
  
  val initialized: Future[Unit] = Future.unit
  
  /** Outer future completes when the transaction is ready to be committed. Inner Future completes when the task completes 
   */
  def prepareTask(taskType: DurableTaskType, initialState: List[(Key, Array[Byte])])(implicit tx: Transaction): Future[Future[Unit]] = synchronized {
    val ts = tx.timestamp()
    val taskTypeArr = uuid2byte(taskType.typeUUID)
    val content = Insert(DurableTask.TaskTypeKey, taskTypeArr, ts) :: initialState.map(t => new Insert(t._1, t._2, ts))

    def allocateIdleTask(): Future[IdleTask] = {
      if (idleTasks.isEmpty) {
        maxTaskNum += 1
        idleTaskAllocater.allocate(maxTaskNum)
      } else {
        val it = idleTasks.head
        idleTasks = idleTasks.tail
        Future.successful(it)
      }
    }
    
    def addTaskAllocationToTransaction(it: IdleTask, taskPromise: Promise[Unit]): Unit = {
      tx.overwrite(it.taskPointer.kvPointer, it.revision, Nil, content)
      
      tx.result.onComplete {
        case Failure(reason) =>
          // Re-read the task revision to ensure we have the up-to-date version before putting it back into the idleTask list
          system.retryStrategy.retryUntilSuccessful { system.readObject(it.taskPointer.kvPointer) } foreach { kvos =>
            synchronized {
              idleTasks = IdleTask(it.taskNumber, it.taskPointer, kvos.revision) :: idleTasks
            }
          }
          
        case Success(_) => synchronized {
          val imap: Map[Key,Value] = Map((DurableTask.TaskTypeKey -> Value(DurableTask.TaskTypeKey, taskTypeArr, ts)))
          val contents = initialState.foldLeft(imap)((m, t) => m + (t._1 -> Value(t._1, t._2, ts)))
          
          val task = taskType.createTask(system, it.taskPointer, tx.txRevision, contents)

          executeTask(ActiveTask(it.taskNumber, it.taskPointer, task))
          
          taskPromise completeWith task.completed.map(_ => ())
        }
      }
    }
      
    allocateIdleTask() map { it =>
      val taskPromise = Promise[Unit]()
      addTaskAllocationToTransaction(it, taskPromise) 
      taskPromise.future
    } 
  }
  
  
}