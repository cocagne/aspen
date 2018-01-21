package com.ibm.aspen.core.data_store

import com.ibm.aspen.core.objects.StorePointer
import java.util.UUID

/** Identifies an object within a DataStore */
case class StoreObjectID(objectUUID: UUID, storePointer: StorePointer)