package com.ibm.aspen.core.objects

import java.util.UUID

/** Object Revisions are set to the UUID of the transaction that last updated them
 * 
 */
final class ObjectRevision(val lastUpdateTxUUID: UUID) extends AnyVal

object ObjectRevision {
  def apply(lastUpdateTxUUID: UUID): ObjectRevision = new ObjectRevision(lastUpdateTxUUID)
  
  val Null = ObjectRevision(new UUID(0,0))
}