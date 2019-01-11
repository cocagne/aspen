package com.ibm.aspen.base.impl

import java.util.UUID

import com.github.blemale.scaffeine.{Cache, Scaffeine}
import com.ibm.aspen.base.ObjectCache
import com.ibm.aspen.core.objects._

import scala.concurrent.duration._

class DefaultObjectCache extends ObjectCache {

  private val cache: Cache[UUID,ObjectState] = Scaffeine()
    .expireAfterWrite(Duration(30, SECONDS))
    .maximumSize(500)
    .build[UUID, ObjectState]()

  def get(pointer: DataObjectPointer): Option[DataObjectState] = {
    cache.getIfPresent(pointer.uuid).map(_.asInstanceOf[DataObjectState])
  }

  def get(pointer: KeyValueObjectPointer): Option[KeyValueObjectState] = {
    cache.getIfPresent(pointer.uuid).map(_.asInstanceOf[KeyValueObjectState])
  }

  /** To be called ONLY by read drivers */
  private[aspen] def put(pointer: ObjectPointer, dos: ObjectState): Unit = cache.put(pointer.uuid, dos)
}
