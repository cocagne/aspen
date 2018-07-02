package com.ibm.aspen.base

import scala.concurrent.ExecutionContext
import scala.concurrent.Future
import java.util.UUID
import com.ibm.aspen.core.objects.StorePointer
import com.ibm.aspen.core.HLCTimestamp

object MissedUpdateIterator {
  case class Entry(objectUUID: UUID, storePointer: StorePointer, timestamp: HLCTimestamp)
}

trait MissedUpdateIterator {
  
  /** None if all objects have been iterated through */
  def entry: Option[MissedUpdateIterator.Entry]
  
  def fetchNext()(implicit ec: ExecutionContext): Future[Unit]
  
  def markRepaired()(implicit ec: ExecutionContext): Future[Unit]
}