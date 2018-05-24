package com.ibm.aspen.fuse.protocol.messages

import com.ibm.aspen.fuse.protocol.Reply
import java.nio.ByteBuffer
import com.ibm.aspen.fuse.protocol.ProtocolVersion

/** Some commands use only return codes and therefore rely on a 0 error return status
 *  to indicate success rather than a custom reply type.
 *  
 * Error-only commands:  
 *   unlink, rmdir, rename, flush, release, fsync, fsyncdir, setxattr,
 *   removexattr, setlk
 */
class ErrorOnly private () extends Reply {
  val protocolVersion: ProtocolVersion = ProtocolVersion(0,0)
  
  def staticReplyLength: Int = throw new AssertionError()
  
  /** writes the reply message to the ByteBuffer (not any following data) */
  private[protocol] def write(bb:ByteBuffer): Unit = throw new AssertionError()
}