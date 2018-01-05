package com.ibm.aspen.base

import java.util.UUID
import scala.annotation.tailrec

class AggregateTaskGroupTypeRegistry(val subregistries: List[TaskGroupTypeRegistry]) extends TaskGroupTypeRegistry {
  def getTaskGroupType(typeUUID: UUID): Option[TaskGroupType] = {
    
    @tailrec
    def rfind(l: List[TaskGroupTypeRegistry]): Option[TaskGroupType] = if (l.isEmpty) None else {
      l.head.getTaskGroupType(typeUUID) match {
        case None => rfind(l.tail)
        case Some(tgt) => Some(tgt)
      }
    }
    
    rfind(subregistries)
  }
}