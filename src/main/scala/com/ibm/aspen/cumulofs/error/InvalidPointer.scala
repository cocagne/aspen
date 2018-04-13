package com.ibm.aspen.cumulofs.error

/** Thrown when decoding a pointer that does not have an expected/supported type code*/
case class InvalidPointer(typeCode: Byte) extends CumuloFSError