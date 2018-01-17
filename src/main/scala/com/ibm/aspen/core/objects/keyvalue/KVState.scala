package com.ibm.aspen.core.objects.keyvalue

import com.ibm.aspen.core.HLCTimestamp

case class KVState(key: Array[Byte], value: Array[Byte], timestamp: HLCTimestamp)