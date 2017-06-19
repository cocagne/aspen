package com.ibm.aspen.core.data_store

object ObjectAllocationError extends Enumeration {
  /** Out of space */
  val OutOfSpace = Value("OutOfSpace")
  
  /** Requested object size exceeds the maximum object size supported by the store */
  val ObjectTooLarge = Value("ObjectTooLarge")
}