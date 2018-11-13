package com.ibm.aspen.core

import scala.concurrent.duration._

final class HLCTimestamp private (private val longValue: Long) extends AnyVal {
  def asLong: Long = longValue
  def wallTime: Long = longValue >> 16
  def logical: Byte = (longValue & 0xFFFFL).asInstanceOf[Byte]
  def asDuration: Duration = Duration(wallTime, MILLISECONDS)
  
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
  
  override def toString: String = s"HLCTimestamp($longValue)"
}

object HLCTimestamp {
  
  private def currentWallTime: Long = System.currentTimeMillis() << 16

  private[this] var lastObserved: HLCTimestamp = HLCTimestamp(currentWallTime)

  def update(seen: HLCTimestamp): Unit = synchronized {
    if (seen > lastObserved)
      lastObserved = seen
  }

  def now: HLCTimestamp = HLCTimestamp()

  val Zero = HLCTimestamp(0)

  def apply(longValue: Long): HLCTimestamp = new HLCTimestamp(longValue)

  def apply(): HLCTimestamp = synchronized { happensAfter(lastObserved) }

  def happensAfter(ts: HLCTimestamp): HLCTimestamp = {
    val n = HLCTimestamp(currentWallTime)

    val next = if (n > ts)
      n
    else {
      val wall = ts.asLong & ~0xFFFFL
      val logical = (ts.asLong & 0xFFFF) + 1
      HLCTimestamp(wall | logical)
    }

    if (!(next > ts)) {
      println(s"INVALID HLC!!!! Old $ts New $next")
    }

    next
  }
}