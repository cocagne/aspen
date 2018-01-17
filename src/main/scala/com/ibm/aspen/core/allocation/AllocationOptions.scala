package com.ibm.aspen.core.allocation

sealed abstract class AllocationOptions

class DataAllocationOptions extends AllocationOptions

class KeyValueAllocationOptions extends AllocationOptions
 