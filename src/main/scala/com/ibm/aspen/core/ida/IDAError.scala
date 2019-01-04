package com.ibm.aspen.core.ida

import com.ibm.aspen.base.AspenError

sealed abstract class IDAError extends AspenError

/** Thrown when an unknown IDA type is found embedded within a serialized ObjectPointer */
class IDAEncodingError extends IDAError

class IDARestoreError extends IDAError

class IDANotSupportedError extends IDAError