package com.ibm.aspen.base.tieredlist

import scala.concurrent.ExecutionContext
import scala.concurrent.Future

class StaticTieredKeyValueRootManager(val root: TieredKeyValueListRoot) extends TieredKeyValueListRootManager {
  def refresh(implicit ec: ExecutionContext): Future[TieredKeyValueListRoot] = Future.successful(root) 
}

object StaticTieredKeyValueRootManager {
  def apply(root: TieredKeyValueListRoot): StaticTieredKeyValueRootManager = new StaticTieredKeyValueRootManager(root)
}