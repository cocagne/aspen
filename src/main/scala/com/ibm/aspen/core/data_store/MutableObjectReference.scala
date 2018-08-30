package com.ibm.aspen.core.data_store

import java.util.UUID

/** Represents a reference to a MutableObject. While one or more references to a MutableObject are held, it
 *  is guaranteed that the object will remain in-memory
 */
class MutableObjectReference(val uuid: UUID) extends AnyVal