package com.ibm.aspen.cumulofs.nfs

import com.ibm.aspen.cumulofs.FIFO

import scala.concurrent.ExecutionContext

class NFSFIFO(val file: FIFO)(implicit ec: ExecutionContext) extends NFSBaseFile {

}
