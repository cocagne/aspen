package com.ibm.aspen.base

import java.util.UUID

trait TaskTypeRegistry {
  def getTaskType(taskTypeUUID: UUID): Option[TaskType]
}