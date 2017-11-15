package com.ibm.aspen.core.transaction

import java.util.UUID
import com.ibm.aspen.core.DataBuffer


/** Identifies the data associated with a DataUpdate in a TransactionDescription that is specific
 *   to the DataStore it's sent to.
 */
case class LocalUpdate(objectUUID: UUID, data: DataBuffer)