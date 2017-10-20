package com.ibm.aspen.base

case class StopRetrying(val reason: Throwable) extends Exception("StopRetrying")