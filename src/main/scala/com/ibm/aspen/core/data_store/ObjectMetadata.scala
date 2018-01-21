package com.ibm.aspen.core.data_store

import com.ibm.aspen.core.HLCTimestamp
import com.ibm.aspen.core.objects.ObjectRefcount
import com.ibm.aspen.core.objects.ObjectRevision
import java.util.UUID

case class ObjectMetadata(
    revision: ObjectRevision,
    refcount: ObjectRefcount,
    timestamp: HLCTimestamp)