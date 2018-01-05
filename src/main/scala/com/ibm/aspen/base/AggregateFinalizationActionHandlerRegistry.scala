package com.ibm.aspen.base

import java.util.UUID
import scala.annotation.tailrec

class AggregateFinalizationActionHandlerRegistry(val subregistries: List[FinalizationActionHandlerRegistry]) extends FinalizationActionHandlerRegistry {
  
  def getFinalizationActionHandler(finalizationActionUUID: UUID): Option[FinalizationActionHandler] = {
    
    @tailrec
    def rfind(l: List[FinalizationActionHandlerRegistry]): Option[FinalizationActionHandler] = if (l.isEmpty) None else {
      l.head.getFinalizationActionHandler(finalizationActionUUID) match {
        case None => rfind(l.tail)
        case Some(tgt) => Some(tgt)
      }
    }
    
    rfind(subregistries)
  }
}