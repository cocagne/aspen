package com.ibm.aspen.core.allocation

import com.ibm.aspen.core.objects.keyvalue.Key
import com.ibm.aspen.core.objects.{KeyValueObjectPointer, ObjectPointer, ObjectRevision}

sealed abstract class AllocationRevisionGuard {
  val pointer: ObjectPointer
  val revision: ObjectRevision
}

case class ObjectAllocationRevisionGuard(
                                          pointer: ObjectPointer,
                                          revision: ObjectRevision
                                        ) extends AllocationRevisionGuard

case class KeyValueAllocationRevisionGuard(
                                            pointer: KeyValueObjectPointer,
                                            key: Key,
                                            revision: ObjectRevision
                                          ) extends AllocationRevisionGuard