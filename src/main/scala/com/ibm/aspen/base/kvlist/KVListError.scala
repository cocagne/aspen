package com.ibm.aspen.base.kvlist

import com.ibm.aspen.core.objects.ObjectPointer

sealed class KVListError(val msg: String) extends Throwable(msg)

/** Thrown when an fetch attempt is made for a key that is below the minimum value of the
 *  list node 
 */
class KeyOutOfRange extends KVListError("KeyOutOfRange")

/** Thrown if an insert exceeds the capacity of a split operation */
class InsertOverflow extends KVListError("InsertOverflow")

/** Thrown if a key or value is too large to encode */
class EncodingSizeError extends KVListError("EncodingSizeError")
