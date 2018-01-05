package com.ibm.aspen.base

import java.util.UUID

trait FinalizationActionHandlerRegistry {
  def getFinalizationActionHandler(finalizationActionUUID: UUID): Option[FinalizationActionHandler]
}