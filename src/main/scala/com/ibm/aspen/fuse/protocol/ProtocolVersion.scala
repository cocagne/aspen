package com.ibm.aspen.fuse.protocol

case class ProtocolVersion(major: Int, minor: Int) extends Ordered[ProtocolVersion] {
  def compare(that: ProtocolVersion): Int = {
    val mjr = major - that.major
    val mnr = minor - that.minor
    if (mjr != 0)
      mjr
    else
      mnr
  }
}
