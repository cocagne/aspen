package com.ibm.aspen.base

import scala.concurrent.ExecutionContext
import scala.concurrent.Future
import java.util.UUID

trait MissedUpdateIterator {
  
  /** None if all objects have been iterated through */
  def objectUUID: Option[UUID]
  
  def fetchNext()(implicit ec: ExecutionContext): Future[Unit]
  
  def markRepaired()(implicit ec: ExecutionContext): Future[Unit]
}