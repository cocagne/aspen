package com.ibm.aspen.base.keyvalue

import com.ibm.aspen.core.objects.keyvalue.Key

sealed abstract class KeyValueError extends Exception

class BelowMinimumError(minimum: Key, attempted: Key) extends KeyValueError

class OutOfRange extends KeyValueError

class CorruptedLinkedList extends KeyValueError
