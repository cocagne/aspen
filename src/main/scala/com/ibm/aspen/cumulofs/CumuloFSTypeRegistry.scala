package com.ibm.aspen.cumulofs

import com.ibm.aspen.base.TypeRegistry
import com.ibm.aspen.base.TypeFactory
import java.util.UUID

object CumuloFSTypeRegistry extends TypeRegistry {
  
  val registry = Map( 
      (CreateFileTask.TaskType.typeUUID -> CreateFileTask.TaskType),
      (DeleteFileTask.TaskType.typeUUID -> DeleteFileTask.TaskType)
      )
      
  override def getTypeFactory[T <: TypeFactory](typeUUID: UUID): Option[T] = registry.get(typeUUID).map(tf => tf.asInstanceOf[T])
}