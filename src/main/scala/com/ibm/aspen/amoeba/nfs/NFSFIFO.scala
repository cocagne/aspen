package com.ibm.aspen.amoeba.nfs

import com.ibm.aspen.amoeba.FIFO

import scala.concurrent.ExecutionContext

class NFSFIFO(val file: FIFO)(implicit ec: ExecutionContext) extends NFSBaseFile {

}
