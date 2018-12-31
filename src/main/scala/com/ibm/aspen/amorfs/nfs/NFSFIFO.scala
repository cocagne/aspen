package com.ibm.aspen.amorfs.nfs

import com.ibm.aspen.amorfs.FIFO

import scala.concurrent.ExecutionContext

class NFSFIFO(val file: FIFO)(implicit ec: ExecutionContext) extends NFSBaseFile {

}
