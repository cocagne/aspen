package com.ibm.aspen.core.transaction

import com.ibm.aspen.core.objects.ObjectRevision
import com.ibm.aspen.core.objects.ObjectRefcount
import java.util.UUID
import com.ibm.aspen.core.HLCTimestamp


case class UpdateErrorResponse(
    objectUUID: UUID,
    updateError: UpdateError.Value,
    currentRevision: Option[ObjectRevision],
    currentRefcount: Option[ObjectRefcount],
    conflictingTransaction: Option[(UUID, HLCTimestamp)])