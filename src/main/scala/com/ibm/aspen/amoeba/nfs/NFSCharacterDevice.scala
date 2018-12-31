package com.ibm.aspen.amoeba.nfs

import com.ibm.aspen.amoeba.CharacterDevice

import scala.concurrent.ExecutionContext

class NFSCharacterDevice(val file: CharacterDevice)(implicit ec: ExecutionContext) extends NFSBaseFile {

  def rdev: Int = file.rdev

  def setrdev(newrdev: Int): Unit = {
    retryTransactionUntilSuccessful(file.fs.system) { implicit tx =>
      file.setrdev(newrdev)
    }
  }
}
