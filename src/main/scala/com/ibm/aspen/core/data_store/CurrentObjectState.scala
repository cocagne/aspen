package com.ibm.aspen.core.data_store

import java.util.UUID
import com.ibm.aspen.core.objects.ObjectRevision
import com.ibm.aspen.core.objects.ObjectRefcount
import com.ibm.aspen.core.transaction.TransactionDescription

case class CurrentObjectState(
    uuid: UUID,
    revision: ObjectRevision,
    refcount: ObjectRefcount,
    lastCommittedTxUUID: UUID,
    lockedTransaction: Option[TransactionDescription])