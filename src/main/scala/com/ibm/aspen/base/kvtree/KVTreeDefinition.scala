package com.ibm.aspen.base.kvtree

import java.util.UUID
import com.ibm.aspen.core.objects.ObjectPointer
import com.ibm.aspen.core.objects.DataObjectPointer

case class KVTreeDefinition(allocationPolicyUUID: UUID, keyComparison: KVTree.KeyComparison.Value, tiers: List[DataObjectPointer])