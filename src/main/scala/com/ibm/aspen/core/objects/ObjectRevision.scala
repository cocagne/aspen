package com.ibm.aspen.core.objects

case class ObjectRevision(overwriteCount: Int, currentSize: Int) extends Ordered[ObjectRevision] {
  def compare(that: ObjectRevision) = {
    val cdiff = overwriteCount - that.overwriteCount
    
    if (cdiff != 0) cdiff else currentSize - that.currentSize
  }
  
  def append(numBytes: Int): ObjectRevision = ObjectRevision(overwriteCount, currentSize + numBytes)
  def overwrite(numBytes: Int): ObjectRevision = ObjectRevision(overwriteCount + 1, numBytes)
  def versionBump(): ObjectRevision = ObjectRevision(overwriteCount+1, currentSize)
}