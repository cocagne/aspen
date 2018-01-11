package com.ibm.aspen.core.objects

case class ObjectRevision(updateCount: Long) extends Ordered[ObjectRevision] {
  def compare(that: ObjectRevision) = (updateCount - that.updateCount).asInstanceOf[Int]
  
  def nextRevision: ObjectRevision = ObjectRevision(updateCount+1)
}