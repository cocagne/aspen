package com.ibm.aspen.cumulofs.impl

import java.util.UUID

import com.ibm.aspen.base.{TypeFactory, TypeRegistry}

object CumuloFSTypeRegistry extends TypeRegistry {

  val registry = Map(
      CreateFileTask.TaskType.typeUUID                     -> CreateFileTask.TaskType,
      DeleteFileTask.TaskType.typeUUID                     -> DeleteFileTask.TaskType,
      IndexedFileContent.DeleteIndexTask.TaskType.typeUUID -> IndexedFileContent.DeleteIndexTask.TaskType,
      SimpleDirectoryTLRootManager.typeUUID                -> SimpleDirectoryTLRootManager
      )

  override def getTypeFactory[T <: TypeFactory](typeUUID: UUID): Option[T] = registry.get(typeUUID).map(tf => tf.asInstanceOf[T])
}
