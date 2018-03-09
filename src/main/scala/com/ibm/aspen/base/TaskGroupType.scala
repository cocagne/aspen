package com.ibm.aspen.base

import java.util.UUID
import com.ibm.aspen.core.objects.ObjectPointer
import scala.concurrent.ExecutionContext
import scala.concurrent.Future
import com.ibm.aspen.core.objects.DataObjectPointer

trait TaskGroupType extends TypeFactory {
  val typeUUID: UUID
  
}