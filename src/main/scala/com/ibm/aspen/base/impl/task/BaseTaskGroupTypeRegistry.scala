package com.ibm.aspen.base.impl.task

import com.ibm.aspen.base.TaskGroupTypeRegistry
import java.util.UUID
import com.ibm.aspen.base.TaskGroupType

object BaseTaskGroupTypeRegistry extends TaskGroupTypeRegistry {
  
  val groups = Map( 
      (SimpleTaskGroupType.groupTypeUUID -> SimpleTaskGroupType) 
  )
  
  def getTaskGroupType(typeUUID: UUID): Option[TaskGroupType] = groups.get(typeUUID)
}