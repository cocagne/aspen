package com.ibm.aspen.fuse.protocol

sealed abstract class ProtocolError extends Exception

/** Thrown durig WireProtocol object construction if the fuse kernel protocol version is not supported */
class UnsupportedFuseProtocolVersion(major: Int, minor: Int) extends ProtocolError

// Thrown if the Fuse connection to the kernel is lost
class LostConnection extends ProtocolError
