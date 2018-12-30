package com.ibm.aspen.base.task

import java.nio.ByteBuffer

import com.ibm.aspen.base.{ObjectReader, Transaction}
import com.ibm.aspen.core.objects.ObjectRevision
import com.ibm.aspen.core.objects.keyvalue.{Delete, Insert, Key, Value}

import scala.concurrent.{ExecutionContext, Future, Promise}

object SteppedDurableTask {

  val ReservedToKeyId: Int = DurableTask.ReservedToKeyId + 5
  
  val StepKey: Key = Key(ReservedToKeyId + 1)
  
  def encodeStep(step: Int): Array[Byte] = {
    val arr = new Array[Byte](4)
    val bb = ByteBuffer.wrap(arr)
    bb.putInt(step)
    arr
  }
  
  def decodeStep(arr: Array[Byte]): Int = ByteBuffer.wrap(arr).getInt()
}

/** Implements a task by way of a sequential set of steps that are transitioned through by way
 *  of successful transaction commits. 
 *  
 *  All updates to the task object should be done through step transitions. 
 * 
 */
abstract class SteppedDurableTask(
    val taskPointer: DurableTaskPointer,
    val reader: ObjectReader,
    initialRevision: ObjectRevision, 
    initialState: Map[Key, Value])(implicit ec: ExecutionContext) extends DurableTask {
  
  import SteppedDurableTask._
  
  private val taskPromise = Promise[(ObjectRevision, Option[AnyRef])]()
  private var currentRevision = initialRevision
  private var currentState: Map[Key, Array[Byte]] = initialState.map(t => t._1 -> t._2.value)
  private var currentStep: Int = initialState.get(StepKey) match {
    case None => 0
    case Some(v) => decodeStep(v.value)
  }
  
  def state: Map[Key,Array[Byte]] = synchronized { currentState }
  
  def step: Int = synchronized { currentStep }
  
  def completed: Future[(ObjectRevision, Option[AnyRef])] = taskPromise.future
  
  def beginStep(): Unit
  
  def resume(): Unit = beginStep()
  
  def refreshTaskState(): Future[Unit] = reader.readObject(taskPointer.kvPointer) map { kvos => synchronized {
    currentState = kvos.contents.map(t => t._1 -> t._2.value)
    currentRevision = kvos.revision
    currentStep = initialState.get(StepKey) match {
      case None => 0
      case Some(v) => v.value(0)
    }
  }}
  
  def completeStep(tx: Transaction, taskStateUpdates: List[(Key,Array[Byte])], taskStateDeletes: List[Key]=Nil): Unit = synchronized {
    
    val nextStep = StepKey -> encodeStep(currentStep+1)
    val deleteSet = taskStateDeletes.toSet
    val newContent = taskStateUpdates.foldLeft(currentState.filter(t => !deleteSet.contains(t._1)))((m, t) => m + t) + nextStep
          
    val updateOps = taskStateDeletes.map(Delete(_)) ++ taskStateUpdates.map(t => Insert(t._1, t._2))
    
    tx.update(taskPointer.kvPointer, Some(currentRevision), Nil, updateOps)

    tx.result foreach { _ => synchronized {
      currentStep += 1
      currentState = newContent
      currentRevision = tx.txRevision
      beginStep()
    }}
  }
  
  def completeTask(tx: Transaction, result: Option[AnyRef]=None): Unit = synchronized {
    val idleTask = new Array[Byte](16) // Zeroed Type UUID

    val resetOps = Insert(DurableTask.TaskTypeKey, idleTask) :: currentState.toList.filter(t => t._1 != DurableTask.TaskTypeKey).map(t => Delete(t._1))

    tx.update(taskPointer.kvPointer, Some(currentRevision), Nil, resetOps)

    tx.result foreach { _ => synchronized {
      taskPromise.success((tx.txRevision, result))
    }}
  }

  def failTask(tx: Transaction, error: Throwable): Unit = synchronized {
    val idleTask = new Array[Byte](16) // Zeroed Type UUID

    val resetOps = Insert(DurableTask.TaskTypeKey, idleTask) :: currentState.toList.filter(t => t._1 != DurableTask.TaskTypeKey).map(t => Delete(t._1))

    tx.update(taskPointer.kvPointer, Some(currentRevision), Nil, resetOps)

    tx.result foreach { _ => synchronized {
      taskPromise.failure(error)
    }}
  }
}