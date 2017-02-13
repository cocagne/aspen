package com.ibm.aspen.core.transaction

object DataUpdateOperation extends Enumeration {
  val Append    = Value("Append")
  val Overwrite = Value("Overwrite")
}