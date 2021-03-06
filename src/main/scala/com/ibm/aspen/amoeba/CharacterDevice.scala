package com.ibm.aspen.amoeba

import scala.concurrent.ExecutionContext
import scala.concurrent.Future

trait CharacterDevice extends BaseFile {
  val pointer: CharacterDevicePointer
  
  def rdev: Int
  
  def setrdev(newrdev: Int)(implicit ec: ExecutionContext): Future[Unit]
}