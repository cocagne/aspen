package com.ibm.aspen.core.allocation

import com.ibm.aspen.core.objects.ObjectRefcount
import com.ibm.aspen.core.objects.ObjectRevision
import java.util.UUID
import com.ibm.aspen.core.read.ReadError
import com.ibm.aspen.core.transaction.TransactionDescription

object AllocationObjectStatus {
  case class State(revision: ObjectRevision,
    refcount: ObjectRefcount,
    lastCommittedTransaction: UUID,
    lockedTransaction: Option[TransactionDescription])
}

case class AllocationObjectStatus(
    uuid: UUID,
    state: Either[ReadError.Value, AllocationObjectStatus.State])