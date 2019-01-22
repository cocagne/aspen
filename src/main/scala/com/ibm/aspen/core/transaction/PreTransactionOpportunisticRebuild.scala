package com.ibm.aspen.core.transaction

import java.util.UUID

import com.ibm.aspen.core.DataBuffer
import com.ibm.aspen.core.data_store.ObjectMetadata

case class PreTransactionOpportunisticRebuild(objectUUID: UUID, metadata: ObjectMetadata, data: DataBuffer)
