package com.ibm.aspen.base.tieredlist

import com.ibm.aspen.base.AspenError
import com.ibm.aspen.core.objects.keyvalue.Key

sealed abstract class KeyValueError extends AspenError

class BelowMinimumError(minimum: Key, attempted: Key) extends KeyValueError

class OutOfRange extends KeyValueError

class CorruptedLinkedList extends KeyValueError

/** Thrown when a single split is insufficient to insert all requested content */
class NodeSizeExceeded extends KeyValueError

class KeyDoesNotExist(val key: Key) extends KeyValueError

class InvalidRoot extends KeyValueError

class TierAlreadyCreated extends KeyValueError

class InvalidConfiguration extends KeyValueError

class EmptyTree extends KeyValueError
