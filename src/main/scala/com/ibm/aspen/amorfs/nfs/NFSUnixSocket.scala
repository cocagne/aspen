package com.ibm.aspen.amorfs.nfs

import com.ibm.aspen.amorfs.UnixSocket

import scala.concurrent.ExecutionContext

class NFSUnixSocket(val file: UnixSocket)(implicit ec: ExecutionContext) extends NFSBaseFile  {

}
