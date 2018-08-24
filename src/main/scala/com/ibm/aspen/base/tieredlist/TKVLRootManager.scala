package com.ibm.aspen.base.tieredlist

import com.ibm.aspen.base.ObjectReader
import com.ibm.aspen.core.objects.keyvalue.Key
import scala.concurrent.ExecutionContext
import scala.concurrent.Future

/** Manages a TieredKeyValueListRoot stored within a TieredKeyValueList
 */
class TKVLRootManager(
    val reader: ObjectReader,
    val containingTKVL: TieredKeyValueList,
    val treeKey: Key,
    initialRoot: TieredKeyValueListRoot) extends TieredKeyValueListRootManager {
  
  protected var troot = initialRoot
  
  def root: TieredKeyValueListRoot = synchronized { troot }
  
  def refresh(implicit ec: ExecutionContext): Future[TieredKeyValueListRoot] = containingTKVL.get(treeKey).map { ov => ov match {
    case None => throw new InvalidRoot
    case Some(v) => synchronized {
      troot = TieredKeyValueListRoot(v.value)
      troot
    }
  }}
}