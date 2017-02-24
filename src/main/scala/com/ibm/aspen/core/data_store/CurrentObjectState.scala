package com.ibm.aspen.core.data_store

import java.util.UUID
import com.ibm.aspen.core.objects.ObjectRevision
import com.ibm.aspen.core.objects.ObjectRefcount

case class CurrentObjectState(
    uuid: UUID,
    revision: ObjectRevision,
    refcount: ObjectRefcount)