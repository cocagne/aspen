package com.ibm.aspen.core.data_store

import java.util.UUID
import com.ibm.aspen.core.objects.ObjectRevision
import com.ibm.aspen.core.objects.ObjectRefcount
import com.ibm.aspen.core.transaction.TransactionDescription
import com.ibm.aspen.core.HLCTimestamp

case class CurrentObjectState(
    uuid: UUID,
    revision: ObjectRevision,
    refcount: ObjectRefcount,
    timestamp: HLCTimestamp,
    lockedTransaction: Option[TransactionDescription])