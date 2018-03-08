package com.ibm.aspen.base.impl.task

import java.util.UUID
import com.ibm.aspen.base.TaskGroupType
import com.ibm.aspen.base.TypeRegistry

object BaseTaskGroupTypeRegistry extends TypeRegistry[TaskGroupType] {
  
  val groups = Map( 
      (SimpleTaskGroupType.typeUUID -> SimpleTaskGroupType) 
  )
  
  def getTypeFactory(typeUUID: UUID): Option[TaskGroupType] = groups.get(typeUUID)
}