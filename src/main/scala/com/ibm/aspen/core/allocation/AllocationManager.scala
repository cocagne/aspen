package com.ibm.aspen.core.allocation

import com.ibm.aspen.core.network.Messenger
import scala.concurrent.ExecutionContext

class AllocationManager(
    val messenger: Messenger,
    val driverFactory: AllocationDriver.Factory)(implicit ec: ExecutionContext) {
  
}