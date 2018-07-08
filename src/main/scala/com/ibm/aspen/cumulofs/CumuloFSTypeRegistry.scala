package com.ibm.aspen.cumulofs

import com.ibm.aspen.base.TypeRegistry
import com.ibm.aspen.base.TypeFactory
import java.util.UUID
import com.ibm.aspen.cumulofs.impl.DeleteTruncatedFileTask

object CumuloFSTypeRegistry extends TypeRegistry {
  
  val registry = Map( 
      (CreateFileTask.TaskType.typeUUID          -> CreateFileTask.TaskType),
      (DeleteFileTask.TaskType.typeUUID          -> DeleteFileTask.TaskType),
      (DeleteTruncatedFileTask.TaskType.typeUUID -> DeleteTruncatedFileTask.TaskType)
      )
      
  override def getTypeFactory[T <: TypeFactory](typeUUID: UUID): Option[T] = registry.get(typeUUID).map(tf => tf.asInstanceOf[T])
}