package com.ibm.aspen.base

import com.ibm.aspen.core.objects._

trait ObjectCache {
  def get(pointer: DataObjectPointer): Option[DataObjectState]
  def get(pointer: KeyValueObjectPointer): Option[KeyValueObjectState]

  /** To be called ONLY by read drivers */
  private[aspen] def put(pointer: ObjectPointer, dos: ObjectState): Unit
}

object ObjectCache {
  object NoCache extends ObjectCache {
    def get(pointer: DataObjectPointer): Option[DataObjectState] = None
    def get(pointer: KeyValueObjectPointer): Option[KeyValueObjectState] = None

    private[aspen] def put(pointer: ObjectPointer, dos: ObjectState): Unit = ()
  }
}
