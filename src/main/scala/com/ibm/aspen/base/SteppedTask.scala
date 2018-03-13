package com.ibm.aspen.base

import com.ibm.aspen.core.objects.KeyValueObjectState
import scala.concurrent.Promise
import scala.concurrent.ExecutionContext
import com.ibm.aspen.core.objects.keyvalue.Key
import scala.concurrent.Future
import java.nio.ByteBuffer
import scala.util.Failure
import scala.util.Success
import com.ibm.aspen.core.objects.keyvalue.Insert

object SteppedTask {
  val StepKey = Key(Array[Byte](1))
  
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
abstract class SteppedTask(
    val taskPointer: TaskPointer,
    val reader: ObjectReader,
    initialState: KeyValueObjectState)(implicit ec: ExecutionContext) extends Task {
  
  import SteppedTask._
  
  private val taskPromise = Promise[Unit]()
  private var currentRevision = initialState.revision
  private var currentState = initialState.contents.map(t => (t._1 -> t._2.value))
  private var currentStep: Int = initialState.contents.get(StepKey) match {
    case None => 0
    case Some(v) => decodeStep(v.value)
  }
  
  def state: Map[Key,Array[Byte]] = synchronized { currentState }
  
  def step: Int = synchronized { currentStep }
  
  def completed: Future[Unit] = taskPromise.future
  
  def beginStep(): Unit
  
  def resume(): Unit = beginStep()
  
  def refreshTaskState(): Future[Unit] = reader.readObject(taskPointer.kvPointer) map { kvos => synchronized {
    currentState = kvos.contents.map(t => (t._1 -> t._2.value))
    currentRevision = kvos.revision
    currentStep = initialState.contents.get(StepKey) match {
      case None => 0
      case Some(v) => v.value(0)
    }
  }}
  
  def completeStep(tx: Transaction, taskStateUpdates: List[(Key,Array[Byte])]): Unit = synchronized {
          
    val newContent = (currentState.iterator ++ taskStateUpdates.iterator).toMap + (StepKey -> encodeStep(currentStep+1))
    val ts = tx.timestamp()
    val ilist = newContent.iterator.map(t => Insert(t._1, t._2, ts)).toList
    
    tx.overwrite(taskPointer.kvPointer, currentRevision, Nil, ilist)

    tx.result foreach { _ => synchronized {
      currentStep += 1
      currentState = newContent
      currentRevision = tx.txRevision
      beginStep()
    }}
  }
  
  def completeTask(tx: Transaction): Unit = synchronized {
    val idleTask = new Array[Byte](16) // Zeroed Type UUID
    tx.overwrite(taskPointer.kvPointer, currentRevision, Nil, List(Insert(Task.TaskTypeKey, idleTask, tx.timestamp())))
    
    tx.result foreach { _ => synchronized { 
      taskPromise.success(())
    }}
  }
}