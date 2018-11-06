package com.ibm.aspen.base.task

import java.util.UUID

import com.ibm.aspen.base.{AspenSystem, ObjectAllocater, Transaction}
import com.ibm.aspen.base.tieredlist.{MutableKeyValueObjectRootManager, MutableTieredKeyValueList, SimpleTieredKeyValueListNodeAllocater, TieredKeyValueListRoot}
import com.ibm.aspen.core.objects.{KeyValueObjectPointer, KeyValueObjectState, ObjectPointer, ObjectRevision}
import com.ibm.aspen.core.objects.keyvalue.{Insert, IntegerKeyOrdering, Key, Value}
import com.ibm.aspen.util._

import scala.collection.immutable.Queue
import scala.concurrent.{ExecutionContext, Future, Promise}
import scala.util.{Failure, Success}

object LocalTaskGroup extends TaskGroupType {
  val AllocaterKey: Key = Key(Array[Byte](1))
  val TaskListKey: Key = Key(Array[Byte](2))
  
  val TaskListNodeSize: Int = 16 * 1024 // TODO: Make this configurable?
  val TaskListKVPairLimit: Int = 20     // TODO: Make this configurable?
  
  val typeUUID: UUID = UUID.fromString("f3051f22-9052-4c71-a469-22047b9edd8b")
  
  sealed abstract class ReusableTask {
    val taskNumber: Int
    val taskPointer: DurableTaskPointer
  }
  
  case class IdleTask(taskNumber: Int, taskPointer: DurableTaskPointer, revision: ObjectRevision) extends ReusableTask {
    //println(s"Created IDLE task with state object ${taskPointer.kvPointer.uuid} revision ${revision}")
  }
  case class ActiveTask(taskNumber: Int, taskPointer: DurableTaskPointer, task: DurableTask) extends ReusableTask {
    //println(s"Created ACTIVE task with state object ${taskPointer.kvPointer.uuid} type ${task.getClass.getTypeName}")
  }
  
  /** Allocates a new task group within the context of a transaction. The outter Future completes when the transaction
   *  is ready to be committed.
   */
  def prepareGroupAllocation(
      system: AspenSystem,
      allocatingObject: ObjectPointer, 
      revision: ObjectRevision, 
      objectAllocaterUUID: UUID)(implicit t: Transaction, ec: ExecutionContext): Future[(TaskGroupPointer, Future[LocalTaskGroup])] = {
    
    val content = List(
      Insert(TaskGroupType.GroupTypeKey, uuid2byte(typeUUID)),
      Insert(AllocaterKey, uuid2byte(objectAllocaterUUID)))
    
    val allocaterType = SimpleTieredKeyValueListNodeAllocater.typeUUID
    val allocaterConfig = SimpleTieredKeyValueListNodeAllocater.encode(Array(objectAllocaterUUID), Array(TaskListNodeSize), Array(TaskListKVPairLimit))
    
    for {
      allocater <- system.getObjectAllocater(objectAllocaterUUID)
      taskListRoot <- allocater.allocateKeyValueObject(allocatingObject, revision, Nil)
      root = TieredKeyValueListRoot(0, IntegerKeyOrdering, taskListRoot, allocaterType, allocaterConfig)
      fullContent = Insert(TaskListKey, root.toArray) :: content
      groupPointer <- allocater.allocateKeyValueObject(allocatingObject, revision, fullContent)

    } yield {
      val ptr = TaskGroupPointer(groupPointer)
      
      val rootMgr = new MutableKeyValueObjectRootManager(system, groupPointer, TaskListKey, root)
      
      val fgroup = t.result.map { _ =>
        new LocalTaskGroup(system, ptr, allocater, new MutableTieredKeyValueList(rootMgr), List())
      }
      
      (ptr, fgroup)
    }
  }
  
  def initializeNewGroup(
      system: AspenSystem,
      groupPointer: TaskGroupPointer, 
      revision: ObjectRevision, 
      objectAllocaterUUID: UUID)(implicit ec: ExecutionContext): Future[LocalTaskGroup] = {
    system.transact { implicit tx =>
      prepareGroupAllocation(system, groupPointer.kvPointer, revision, objectAllocaterUUID)
    }.flatMap(t => t._2)
  }
    
  
  def createInterface(system:AspenSystem, groupState: KeyValueObjectState)(implicit ec: ExecutionContext): Future[LocalTaskGroup] = {
    createExecutor(system, groupState)
  }
  
  def createExecutor(system:AspenSystem, groupState: KeyValueObjectState)(implicit ec: ExecutionContext): Future[LocalTaskGroup] = {

    val objectAllocaterUUID = byte2uuid(groupState.contents(AllocaterKey).value)
    val rootMgr = new MutableKeyValueObjectRootManager(system, groupState.pointer, TaskListKey, TieredKeyValueListRoot(groupState.contents(TaskListKey).value))
    val taskTree = new MutableTieredKeyValueList(rootMgr)
    
    var loadingTasks: List[Future[ReusableTask]] = Nil
    
    def taskVisitor(v: Value): Unit = {
      val taskNumber = v.key.intValue
      val pointer = KeyValueObjectPointer(v.value)
      val ftask = system.readObject(pointer) map { taskState =>
        
        val taskType = taskState.contents.get(DurableTask.TaskTypeKey) match {
          case None => DurableTask.IdleTaskType
          case Some(ttv) => byte2uuid(ttv.value)
        }
        
        if (taskType == DurableTask.IdleTaskType)
          IdleTask(taskNumber, DurableTaskPointer(pointer), taskState.revision)
        else {
          system.typeRegistry.getTypeFactory[DurableTaskType](taskType) match {
            case None =>
              println(s"UNKNOWN TASK TYPE: $taskType")
              throw new Exception("Unknown Task Type") // TODO need a better error handling strategy here
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
    protected val taskTree: MutableTieredKeyValueList,
    initialTasks: List[LocalTaskGroup.ReusableTask])
    (implicit ec: ExecutionContext) extends TaskGroupExecutor with TaskGroupInterface {
    
  val taskGroupType: LocalTaskGroup.type = LocalTaskGroup
  
  import LocalTaskGroup._
  
  private var running = false
  
  protected var maxTaskNum: Int = initialTasks.foldLeft(0)( (max, rt) => if (rt.taskNumber > max) rt.taskNumber else max )
  
  protected var idleTasks: List[IdleTask] = Nil
  protected var activeTasks: Map[Int, ActiveTask] = Map()
  protected var noRemainingTasks: Option[Promise[Unit]] = None
  protected val idleTaskAllocater = new IdleTaskAllocater()
  
  initialTasks.foreach {
    case t: IdleTask => idleTasks = t :: idleTasks
    case t: ActiveTask => activeTasks += (t.taskNumber -> t)
  }
  
  def resume(): Unit = synchronized {
    if (!running) {
      running = true
      
      // Resume all active tasks
      activeTasks.foreach(t => executeTask(t._2))
    }
  }
  
  protected class IdleTaskAllocater {
    private var pending = Queue[(Int, Promise[IdleTask])]()
    private var allocating = false
    
    def allocateNext(): Unit = {
      val ((taskNumber, promise), newPending) = pending.dequeue
      pending = newPending
      
      val f = system.transactUntilSuccessful { implicit tx =>
        for {
          mnode <- taskTree.fetchMutableNode(Key(taskNumber))
          newObject <- allocater.allocateKeyValueObject(mnode.kvos.pointer, mnode.kvos.revision, Nil)
          _ <- mnode.prepreUpdateTransaction(List((Key(taskNumber), newObject.toArray)), Nil, Nil)
        } yield (newObject, tx.txRevision)        
      }
      
      f.foreach { t => synchronized { 
        promise.success(IdleTask(taskNumber, DurableTaskPointer(t._1), t._2))
        allocating = false
        if (pending.nonEmpty)
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
  
  def whenAllTasksComplete(): Future[Unit] = synchronized {
    noRemainingTasks match {
      case Some(p) => p.future
      case None =>
        if (activeTasks.isEmpty)
          Future.unit
        else {
          val p = Promise[Unit]()
          noRemainingTasks = Some(p)
          p.future
        }
    }
  }
  
  protected def executeTask(at: ActiveTask): Unit = synchronized {
    activeTasks += (at.taskNumber -> at)
    at.task.resume()
    at.task.completed foreach { t => synchronized {
      activeTasks -= at.taskNumber
      idleTasks = IdleTask(at.taskNumber, at.taskPointer, t._1) :: idleTasks
      if (activeTasks.isEmpty) {
        noRemainingTasks match {
          case None =>
          case Some(p) => 
            noRemainingTasks = None
            p.success(())
        }
      }
    }}
  }
  
  def shutdown(): Future[Unit] = Future.unit
  
  val initialized: Future[Unit] = Future.unit
  
  /** Outer future completes when the transaction is ready to be committed. Inner Future completes when the task completes 
   */
  override def prepareTask(
      taskType: DurableTaskType, 
      initialState: List[(Key, Array[Byte])])(implicit tx: Transaction): Future[Future[Option[AnyRef]]] = synchronized {
        
    val taskTypeArr = uuid2byte(taskType.typeUUID)
    val content = Insert(DurableTask.TaskTypeKey, taskTypeArr) :: initialState.map(t => new Insert(t._1, t._2))

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
    
    def addTaskAllocationToTransaction(it: IdleTask, taskPromise: Promise[Option[AnyRef]]): Unit = {
      tx.update(it.taskPointer.kvPointer, Some(it.revision), Nil, content)
      
      tx.result.onComplete {
        case Failure(_) =>
          // Re-read the task revision to ensure we have the up-to-date version before putting it back into the idleTask list
          system.readObject(it.taskPointer.kvPointer) foreach { kvos =>
            synchronized {
              idleTasks = IdleTask(it.taskNumber, it.taskPointer, kvos.revision) :: idleTasks
            }
          }
          
        case Success(timestamp) => synchronized {
          
          val ttv = Value(DurableTask.TaskTypeKey, taskTypeArr, timestamp, tx.txRevision)
          val content = initialState.map(t => t._1 -> Value(t._1, t._2, timestamp, tx.txRevision)).toMap + (ttv.key -> ttv)
          val task = taskType.createTask(system, it.taskPointer, tx.txRevision, content)

          executeTask(ActiveTask(it.taskNumber, it.taskPointer, task))
          
          taskPromise completeWith task.completed.map(t => t._2)
        }
      }
    }
      
    allocateIdleTask() map { it =>
      val taskPromise = Promise[Option[AnyRef]]()
      addTaskAllocationToTransaction(it, taskPromise) 
      taskPromise.future
    } 
  }
  
  
}