package com.ibm.aspen.base

import scala.concurrent.ExecutionContext
import scala.concurrent.Future

trait ObjectAllocaterFactory extends TypeFactory {
  def create(system: AspenSystem)(implicit ec: ExecutionContext): Future[ObjectAllocater]
}