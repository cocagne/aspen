package com.ibm.aspen.objects

import java.util.UUID
import com.ibm.aspen.ida.IDA

class ObjectPointer(
    val uuid: UUID,
    val poolUUID: UUID,
    val size: Option[Int],
    val ida: IDA,
    val storePointers: Array[StorePointer]
    ) {
  
}