package com.ibm.aspen.amoeba.nfs

import com.ibm.aspen.amoeba.UnixSocket

import scala.concurrent.ExecutionContext

class NFSUnixSocket(val file: UnixSocket)(implicit ec: ExecutionContext) extends NFSBaseFile  {

}
