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
  
  def now: HLCTimestamp = HLCTimestamp()
  
  def apply(longValue: Long): HLCTimestamp = new HLCTimestamp(longValue)
  
  def apply(): HLCTimestamp = new HLCTimestamp(System.currentTimeMillis() & 0xFFFFFFFFFFFF0000L)
  
  def happensAfter(ts: HLCTimestamp): HLCTimestamp = {
    val now = HLCTimestamp()
    
    if (ts.wallTime == now.wallTime) 
      HLCTimestamp(((now.wallTime << 16) | ts.logical) + 1)
      
    else if (now.wallTime < ts.wallTime)
      HLCTimestamp(((ts.wallTime << 16) | ts.logical) + 1)
      
    else
      now
  }
  
}