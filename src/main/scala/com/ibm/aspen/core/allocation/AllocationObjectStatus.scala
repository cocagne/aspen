package com.ibm.aspen.core.allocation

import com.ibm.aspen.core.objects.ObjectRefcount
import com.ibm.aspen.core.objects.ObjectRevision
import java.util.UUID
import com.ibm.aspen.core.transaction.TransactionDescription
import com.ibm.aspen.core.data_store.Lock
import com.ibm.aspen.core.data_store.ObjectReadError

object AllocationObjectStatus {
  case class State(revision: ObjectRevision,
                   refcount: ObjectRefcount,
                   locks: List[Lock])
}

case class AllocationObjectStatus(
    uuid: UUID,
    state: Either[ObjectReadError.Value, AllocationObjectStatus.State])