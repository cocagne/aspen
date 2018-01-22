package com.ibm.aspen.core.allocation

import com.ibm.aspen.core.objects.ObjectRefcount
import com.ibm.aspen.core.objects.ObjectRevision
import java.util.UUID
import com.ibm.aspen.core.read.ReadError
import com.ibm.aspen.core.transaction.TransactionDescription
import com.ibm.aspen.core.data_store.Lock

object AllocationObjectStatus {
  case class State(revision: ObjectRevision,
                   refcount: ObjectRefcount,
                   locks: List[Lock])
}

case class AllocationObjectStatus(
    uuid: UUID,
    state: Either[ReadError.Value, AllocationObjectStatus.State])