package com.ibm.aspen.core.allocation

sealed abstract class AllocationOptions

class DataAllocationOptions extends AllocationOptions

class KeyValueAllocationOptions(
    val useRevisions: Boolean,
    val useRefcounts: Boolean,
    val useTimestamps: Boolean) extends AllocationOptions
 