package com.ibm.aspen.core

import scala.concurrent.duration._

final class HLCTimestamp private (private val longValue: Long) extends AnyVal {
  def asLong: Long = longValue
  def wallTime: Long = longValue >> 16
  def logical: Byte = (longValue & 0xFFFFL).asInstanceOf[Byte]
  def asDuration: Duration = Duration(asLong, MILLISECONDS)
  
  def compareTo(t: HLCTimestamp): Long = {
    val pdelta = wallTime - t.wallTime
    if (pdelta == 0)
      logical - t.logical
    else
      pdelta
  }
  
  def -(rhs: HLCTimestamp): Duration = this.asDuration - rhs.asDuration
  def >(rhs: HLCTimestamp): Boolean = compareTo(rhs) > 0
  def <(rhs: HLCTimestamp): Boolean = compareTo(rhs) < 0
  
  override def toString(): String = s"HLCTimestamp($longValue)"
}

object HLCTimestamp {
  private[this] var last: HLCTimestamp = HLCTimestamp(System.currentTimeMillis() & 0xFFFFFFFFFFFF0000L)
  
  def update(seen: HLCTimestamp): Unit = synchronized {
    if (seen > last) 
      last = seen
  }
  
  def now: HLCTimestamp = HLCTimestamp()
  
  def apply(longValue: Long): HLCTimestamp = new HLCTimestamp(longValue)
  
  def apply(): HLCTimestamp = synchronized {
    val n = HLCTimestamp(System.currentTimeMillis() & 0xFFFFFFFFFFFF0000L)
    
     if (n > last)
      last = n
    else
      last = HLCTimestamp(((last.wallTime << 16) | last.logical) + 1)
    
    last
  }
  
  def happensAfter(ts: HLCTimestamp): HLCTimestamp = synchronized {
    val n = HLCTimestamp(System.currentTimeMillis() & 0xFFFFFFFFFFFF0000L)
    
    if (ts > last)
      last = ts

    val newTs = if (last.wallTime >= n.wallTime || last.logical > n.logical) 
      HLCTimestamp(((last.wallTime << 16) | last.logical) + 1)
    else
      now
      
    last = newTs
    
    last
  }
}