package com.ibm.aspen.cumulofs

import scala.concurrent.ExecutionContext
import scala.concurrent.Future

trait BlockDevice extends BaseFile {
  val pointer: BlockDevicePointer
  
  def rdev: Int
  
  def setrdev(newrdev: Int)(implicit ec: ExecutionContext): Future[Unit]
}