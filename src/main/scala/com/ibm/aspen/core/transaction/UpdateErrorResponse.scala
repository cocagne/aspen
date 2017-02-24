package com.ibm.aspen.core.transaction

import com.ibm.aspen.core.objects.ObjectRevision
import com.ibm.aspen.core.objects.ObjectRefcount


case class UpdateErrorResponse(
    updateType: UpdateType.Value,
    updateIndex: Byte,
    updateError: UpdateError.Value,
    currentRevision: Option[ObjectRevision],
    currentRefcount: Option[ObjectRefcount],
    conflictingTransaction: Option[TransactionDescription])