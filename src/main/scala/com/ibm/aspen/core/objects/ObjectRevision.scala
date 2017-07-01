package com.ibm.aspen.core.objects

case class ObjectRevision(overwriteCount: Int, currentSize: Int) extends Ordered[ObjectRevision] {
  def compare(that: ObjectRevision) = {
    if (overwriteCount < that.overwriteCount)
      -1
    else if (overwriteCount > that.overwriteCount)
      1
    else
      overwriteCount - that.overwriteCount
  }
}