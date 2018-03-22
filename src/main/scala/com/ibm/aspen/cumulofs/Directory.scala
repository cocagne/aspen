package com.ibm.aspen.cumulofs

import scala.concurrent.Future
import scala.concurrent.ExecutionContext

trait Directory {
  
  val parent: Option[Directory]
  
  def fetchSubdirectory(name: String)(implicit ec: ExecutionContext): Future[Option[Directory]]
}