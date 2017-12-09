package com.ibm.aspen.core.transaction

import com.ibm.aspen.core.objects.ObjectRefcount
import com.ibm.aspen.core.objects.ObjectRevision
import java.util.UUID

case class ObjectStatus(
    uuid: UUID,
    revision: ObjectRevision,
    refcount: ObjectRefcount,
    lastCommittedTransaction: UUID,
    lockedTransaction: Option[TransactionDescription])