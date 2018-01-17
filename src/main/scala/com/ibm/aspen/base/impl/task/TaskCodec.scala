package com.ibm.aspen.base.impl.task

import com.google.flatbuffers.FlatBufferBuilder
import java.nio.ByteBuffer
import com.ibm.aspen.core.network.NetworkCodec
import com.ibm.aspen.base.impl.task.{codec => P}
import com.ibm.aspen.core.objects.ObjectPointer
import java.util.UUID
import com.ibm.aspen.core.DataBuffer
import scala.collection.immutable.Queue
import com.ibm.aspen.core.objects.DataObjectPointer
import java.io.DataOutput
import com.ibm.aspen.core.objects.DataObjectPointer

object TaskCodec {
  
  def encodeTaskGroupTreeEntry(groupTypeUUID: UUID, groupDefinitionPointer: ObjectPointer): Array[Byte] = {
    val builder = new FlatBufferBuilder(1024)
    val off = NetworkCodec.encode(builder, groupDefinitionPointer)
    
    P.TaskGroupTreeEntry.startTaskGroupTreeEntry(builder)
    P.TaskGroupTreeEntry.addTaskGroupTypeUUID(builder, NetworkCodec.encode(builder, groupTypeUUID))
    P.TaskGroupTreeEntry.addTaskGroupDefinitionPointer(builder, off)
    
    val finalOffset = P.TaskGroupTreeEntry.endTaskGroupTreeEntry(builder)
    
    builder.finish(finalOffset)
    
    NetworkCodec.byteBufferToArray(builder.dataBuffer())
  }
  def decodeTaskGroupTreeEntry(arr: Array[Byte]): (UUID, DataObjectPointer) = {
    val n = P.TaskGroupTreeEntry.getRootAsTaskGroupTreeEntry(ByteBuffer.wrap(arr))
    (NetworkCodec.decode(n.taskGroupTypeUUID()), NetworkCodec.decode(n.taskGroupDefinitionPointer()).asInstanceOf[DataObjectPointer])
  }
  
  def encodeTaskCreationFinalizationAction(fa: TaskCreationFinalizationAction.FAContent): Array[Byte] = {
    val builder = new FlatBufferBuilder(1024)
    
    val objectOffset = NetworkCodec.encode(builder, fa.taskObject)
    
    P.TaskCreationFinalizationAction.startTaskCreationFinalizationAction(builder)
    P.TaskCreationFinalizationAction.addTaskGroup(builder, NetworkCodec.encode(builder, fa.taskGroupUUID))
    P.TaskCreationFinalizationAction.addTaskTypeUUID(builder, NetworkCodec.encode(builder, fa.taskTypeUUID))
    P.TaskCreationFinalizationAction.addTaskUUID(builder, NetworkCodec.encode(builder, fa.taskUUID))
    P.TaskCreationFinalizationAction.addTaskObject(builder, objectOffset)
    P.TaskCreationFinalizationAction.addTaskRevision(builder, NetworkCodec.encodeObjectRevision(builder, fa.taskRevision))
    
    val finalOffset = P.TaskCreationFinalizationAction.endTaskCreationFinalizationAction(builder)
    
    builder.finish(finalOffset)
    
    NetworkCodec.byteBufferToArray(builder.dataBuffer())
  }
  
  def decodeTaskCreationFinalizationAction(arr: Array[Byte]): TaskCreationFinalizationAction.FAContent = {
    val n = P.TaskCreationFinalizationAction.getRootAsTaskCreationFinalizationAction(ByteBuffer.wrap(arr))
    
    TaskCreationFinalizationAction.FAContent(
        NetworkCodec.decode(n.taskGroup()),
        NetworkCodec.decode(n.taskTypeUUID()),
        NetworkCodec.decode(n.taskUUID()),
        NetworkCodec.decode(n.taskObject()).asInstanceOf[DataObjectPointer],
        NetworkCodec.decode(n.taskRevision()))
  }
  
  def encode(builder: FlatBufferBuilder, task: TaskDefinition): Int = {
    val offset = NetworkCodec.encode(builder, task.taskObject)
    P.TaskDefinition.startTaskDefinition(builder)
    P.TaskDefinition.addTaskTypeUUID(builder, NetworkCodec.encode(builder, task.taskTypeUUID))
    P.TaskDefinition.addTaskUUID(builder, NetworkCodec.encode(builder, task.taskUUID))
    P.TaskDefinition.addTaskObject(builder, offset)
    P.TaskDefinition.endTaskDefinition(builder)
  }
  def decode(n: P.TaskDefinition): TaskDefinition = {
    NetworkCodec.decode(n.taskObject()) match {
      case d: DataObjectPointer => TaskDefinition(NetworkCodec.decode(n.taskTypeUUID()), NetworkCodec.decode(n.taskUUID()), d)
      case _ => throw new Exception("Unsupported Object Type")
    }
  }
  
  def encodeTaskGroupDefinition(tasks: List[TaskDefinition]): DataBuffer = {
    
    val numTasks = tasks.length
    
    val sizeGuesstimate = (16 + 8 + 100) * numTasks
    
    val builder = new FlatBufferBuilder(sizeGuesstimate)
    
    val tasksOffset = P.TaskGroupDefinition.createTasksVector(builder, tasks.map(t => encode(builder, t)).toArray)
        
    P.TaskGroupDefinition.startTaskGroupDefinition(builder)
    P.TaskGroupDefinition.addTasks(builder, tasksOffset)
    
    builder.finish(P.TaskGroupDefinition.endTaskGroupDefinition(builder))
    
    DataBuffer(builder.dataBuffer())
  }
  def decode(n: P.TaskGroupDefinition): List[TaskDefinition] = {
    var tasks: List[TaskDefinition] = Nil
    
    for (i <- 0 until n.tasksLength())
      tasks = decode(n.tasks(i)) :: tasks
    
    // Reverse list to preserve initial ordering
    tasks.reverse
  }
  def decodeTaskGroupDefinition(db: DataBuffer): List[TaskDefinition] = {
    decode(P.TaskGroupDefinition.getRootAsTaskGroupDefinition(db.asReadOnlyBuffer()))
  }
}