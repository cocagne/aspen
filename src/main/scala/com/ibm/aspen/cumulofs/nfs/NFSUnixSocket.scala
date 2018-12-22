package com.ibm.aspen.cumulofs.nfs

import com.ibm.aspen.cumulofs.UnixSocket

import scala.concurrent.ExecutionContext

class NFSUnixSocket(val file: UnixSocket)(implicit ec: ExecutionContext) extends NFSBaseFile  {

}
