package com.ibm.aspen.base.task

import java.util.UUID
import com.ibm.aspen.core.objects.keyvalue.Key
import com.ibm.aspen.base.TypeFactory
import com.ibm.aspen.base.AspenSystem
import com.ibm.aspen.core.objects.KeyValueObjectState
import scala.concurrent.ExecutionContext
import scala.concurrent.Future
import com.ibm.aspen.core.objects.KeyValueObjectPointer

object TaskGroupType {
  val GroupTypeKey = Key(Array[Byte](0))
}

trait TaskGroupType extends TypeFactory {
  val typeUUID: UUID
  
  def createExecutor(system:AspenSystem, groupState: KeyValueObjectState)(implicit ec: ExecutionContext): Future[TaskGroupExecutor]
  
  def createInterface(system:AspenSystem, groupState: KeyValueObjectState)(implicit ec: ExecutionContext): Future[TaskGroupInterface]
  
  def createExecutor(
      system:AspenSystem, 
      groupStatePointer: KeyValueObjectPointer)(implicit ec: ExecutionContext): Future[TaskGroupExecutor] = {
    system.readObject(groupStatePointer).flatMap( kvos => createExecutor(system, kvos) )
  }
  
  def createInterface(
      system:AspenSystem, 
      groupStatePointer: KeyValueObjectPointer)(implicit ec: ExecutionContext): Future[TaskGroupInterface] = {
    system.readObject(groupStatePointer).flatMap( kvos => createInterface(system, kvos) )
  }
}