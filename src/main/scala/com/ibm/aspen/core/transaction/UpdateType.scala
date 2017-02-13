package com.ibm.aspen.core.transaction

object UpdateType extends Enumeration {
  val Data     = Value("Data")
  val Refcount = Value("Refcount")
}