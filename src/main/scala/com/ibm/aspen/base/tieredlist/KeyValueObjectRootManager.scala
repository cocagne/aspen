package com.ibm.aspen.base.tieredlist

import com.ibm.aspen.core.objects.KeyValueObjectPointer
import com.ibm.aspen.core.objects.keyvalue.Key
import scala.concurrent.ExecutionContext
import scala.concurrent.Future
import com.ibm.aspen.base.ObjectReader

/** Manages a TieredKeyValueListRoot stored within a KeyValue object
 */
class KeyValueObjectRootManager(
    val reader: ObjectReader,
    val containingObject: KeyValueObjectPointer,
    val treeKey: Key,
    initialRoot: TieredKeyValueListRoot) extends TieredKeyValueListRootManager {
  
  protected var troot = initialRoot
  
  def root: TieredKeyValueListRoot = synchronized { troot }
  
  def refresh(implicit ec: ExecutionContext): Future[TieredKeyValueListRoot] = reader.readSingleKey(containingObject, treeKey, root.keyOrdering) map { kvoss =>
    kvoss.contents.get(treeKey) match {
      case None => throw new InvalidRoot
      case Some(v) => synchronized {
        troot = TieredKeyValueListRoot(v.value)
        troot
      }
    }
  }
}