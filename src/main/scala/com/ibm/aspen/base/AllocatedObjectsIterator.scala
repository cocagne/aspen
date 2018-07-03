package com.ibm.aspen.base

import com.ibm.aspen.core.objects.ObjectPointer
import com.ibm.aspen.core.HLCTimestamp
import scala.concurrent.ExecutionContext
import scala.concurrent.Future

object AllocatedObjectsIterator {
  
  /** Note that the timestamp is some arbitrary time *after* the object was actually allocated.
   *  This timestamp represents the transaction time of the insertion of the object into the allocation
   *  tree, not the actual allocation of the object itself. This value will generally be pretty close to
   *  the actual allocation time of the object but reliance upon this should be avoided. Insertion into the
   *  tree is done by way of a finalization action and those can be delayed arbitrarily long in the presence
   *  failures. 
   */
  case class AllocatedObject(pointer: ObjectPointer, timestamp: HLCTimestamp)
}

trait AllocatedObjectsIterator {
  import AllocatedObjectsIterator._
  
   /** None if all of the allocated objects have been iterated through */
  def fetchNext()(implicit ec: ExecutionContext): Future[Option[AllocatedObject]]
}