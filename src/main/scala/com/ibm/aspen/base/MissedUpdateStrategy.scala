package com.ibm.aspen.base

import java.util.UUID

case class MissedUpdateStrategy(strategyUUID: UUID, config: Option[Array[Byte]])