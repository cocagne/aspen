package com.ibm.aspen.base.tieredlist

import scala.concurrent.ExecutionContext
import scala.concurrent.Future

trait TieredKeyValueListRootManager {
  def root: TieredKeyValueListRoot
  
  def refresh(implicit ec: ExecutionContext): Future[TieredKeyValueListRoot] 
}