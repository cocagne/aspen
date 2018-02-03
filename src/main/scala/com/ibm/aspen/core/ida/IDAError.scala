package com.ibm.aspen.core.ida

sealed abstract class IDAError extends Exception

/** Thrown when an unknown IDA type is found embedded within a serialized ObjectPointer */
class IDAEncodingError extends IDAError

class IDARestoreError extends IDAError

class IDANotSupportedError extends IDAError