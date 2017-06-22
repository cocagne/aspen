package com.ibm.aspen.core.allocation

import com.ibm.aspen.core.network.TransactionMessenger
import scala.concurrent.ExecutionContext

class AllocationManager(
    val messenger: TransactionMessenger,
    val driverFactory: AllocationDriver.Factory)(implicit ec: ExecutionContext) {
  
}