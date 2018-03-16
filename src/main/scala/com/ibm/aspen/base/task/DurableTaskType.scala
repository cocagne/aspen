package com.ibm.aspen.base.task

import java.util.UUID
import com.ibm.aspen.base.TypeFactory
import com.ibm.aspen.core.objects.KeyValueObjectState
import com.ibm.aspen.base.AspenSystem
import scala.concurrent.ExecutionContext
import scala.concurrent.Future
import com.ibm.aspen.core.objects.ObjectRevision
import com.ibm.aspen.core.objects.keyvalue.Key
import com.ibm.aspen.core.objects.keyvalue.Value

trait DurableTaskType extends TypeFactory {
  
  val typeUUID: UUID
 
  def createTask(
      system: AspenSystem, 
      pointer: DurableTaskPointer, 
      revision: ObjectRevision, 
      state: Map[Key, Value])(implicit ec: ExecutionContext): DurableTask
  
}