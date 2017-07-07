package com.ibm.aspen.core.objects

case class ObjectRevision(overwriteCount: Int, currentSize: Int) extends Ordered[ObjectRevision] {
  def compare(that: ObjectRevision) = {
    val cdiff = overwriteCount - that.overwriteCount
    
    if (cdiff != 0) cdiff else currentSize - that.currentSize
  }
}