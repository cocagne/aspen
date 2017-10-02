package com.ibm.aspen.core.allocation

object AllocationErrors extends Enumeration{
  /** Insufficient space to allocate object of the requested size */
  val InsufficientSpace = Value("InsufficientSpace")
}