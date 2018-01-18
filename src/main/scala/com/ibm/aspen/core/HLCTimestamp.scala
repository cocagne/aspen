package com.ibm.aspen.core

final class HLCTimestamp private (private val longValue: Long) extends AnyVal {
  def asLong: Long = longValue
  def wallTime: Long = longValue >> 16
  def logical: Byte = (longValue & 0xFFFFL).asInstanceOf[Byte]
  
  def compareTo(t: HLCTimestamp): Long = {
    val pdelta = wallTime - t.wallTime
    if (pdelta == 0)
      logical - t.logical
    else
      pdelta
  }
  
  override def toString(): String = s"HLCTimestamp($longValue)"
}

object HLCTimestamp {
  
  def now: HLCTimestamp = HLCTimestamp()
  
  def apply(longValue: Long): HLCTimestamp = new HLCTimestamp(longValue)
  
  def apply(): HLCTimestamp = new HLCTimestamp(System.currentTimeMillis() & 0xFFFFFFFFFFFF0000L)
  
  def happensAfter(ts: HLCTimestamp): HLCTimestamp = {
    val now = HLCTimestamp()
    
    if (ts.wallTime == now.wallTime) 
      HLCTimestamp(now.wallTime & ts.logical + 1)
      
    else if (now.wallTime < ts.wallTime)
      HLCTimestamp(ts.wallTime & ts.logical + 1)
      
    else
      now
  }
  
}