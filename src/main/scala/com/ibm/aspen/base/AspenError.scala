package com.ibm.aspen.base

abstract class AspenError(message:String = null, cause: Throwable = null) extends Exception(message, cause)

