package com.ibm.aspen.base

import java.util.UUID

import com.ibm.aspen.core.HLCTimestamp
import com.ibm.aspen.core.objects.ObjectPointer

import scala.concurrent.{ExecutionContext, Future}

object MissedUpdateIterator {
  case class Entry(objectUUID: UUID, pointer: ObjectPointer, timestamp: HLCTimestamp)
}

trait MissedUpdateIterator {
  
  /** None if all objects have been iterated through */
  def entry: Option[MissedUpdateIterator.Entry]
  
  def fetchNext()(implicit ec: ExecutionContext): Future[Unit]
  
  def markRepaired()(implicit ec: ExecutionContext): Future[Unit]
}