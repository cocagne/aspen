package com.ibm.aspen.base

import java.util.UUID

trait TaskGroupTypeRegistry {
  def getTaskGroupType(typeUUID: UUID): Option[TaskGroupType]
}