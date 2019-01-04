package com.ibm.aspen.base

case class StopRetrying(reason: Throwable) extends AspenError("StopRetrying", reason)