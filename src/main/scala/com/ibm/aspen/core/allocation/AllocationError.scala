package com.ibm.aspen.core.allocation

object AllocationError extends Enumeration{
  /** Insufficient space to allocate object of the requested size */
  val InsufficientSpace = Value("InsufficientSpace")
}