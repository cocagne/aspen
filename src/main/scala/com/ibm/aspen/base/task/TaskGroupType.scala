package com.ibm.aspen.base.task

import java.util.UUID
import com.ibm.aspen.core.objects.keyvalue.Key
import com.ibm.aspen.base.TypeFactory

object TaskGroupType {
  val GroupTypeKey = Key(Array[Byte](0))
}

trait TaskGroupType extends TypeFactory {
  val typeUUID: UUID
  
}