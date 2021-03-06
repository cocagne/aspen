package com.ibm.aspen.amoeba

import scala.concurrent.Future
import scala.concurrent.ExecutionContext

trait FileLoader {
  def loadFile(fs: FileSystem, pointer: FilePointer)(implicit ec: ExecutionContext): Future[File]
}