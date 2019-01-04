package com.ibm.aspen.base

import java.util.UUID

/** Thrown when the requested pool cannot be found */
class UnknownStoragePool(val poolUUID: UUID) extends AspenError
