package com.ibm.aspen.base

import java.util.UUID
import scala.annotation.tailrec

class AggregateTaskTypeRegistry(val subregistries: List[TaskTypeRegistry]) extends TaskTypeRegistry {
  def getTaskType(typeUUID: UUID): Option[TaskType] = {
    
    @tailrec
    def rfind(l: List[TaskTypeRegistry]): Option[TaskType] = if (l.isEmpty) None else {
      l.head.getTaskType(typeUUID) match {
        case None => rfind(l.tail)
        case Some(tgt) => Some(tgt)
      }
    }
    
    rfind(subregistries)
  }
}