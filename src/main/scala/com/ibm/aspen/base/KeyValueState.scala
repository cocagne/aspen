package com.ibm.aspen.base

import com.ibm.aspen.core.objects.ObjectRevision
import com.ibm.aspen.core.objects.ObjectRefcount
import com.ibm.aspen.core.HLCTimestamp

case class KeyValueState(
    val key: Array[Byte],
    val value: Array[Byte],
    val revision: Option[ObjectRevision], 
    val refcount: Option[ObjectRefcount], 
    val timestamp: Option[HLCTimestamp])