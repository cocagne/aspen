package com.ibm.aspen.base.kvlist

import com.ibm.aspen.base.AspenSystem
import scala.concurrent.ExecutionContext
import com.ibm.aspen.core.objects.ObjectPointer
import scala.concurrent.Future
import com.ibm.aspen.base.ObjectStateAndData

trait KVList {
   
  def fetchNodeObject(objectPointer: ObjectPointer): Future[ObjectStateAndData]
  
  def compareKeys(a: Array[Byte], b: Array[Byte]): Int
  
  val rootObjectPointer: ObjectPointer
  
  val objectAllocater: KVListNodeAllocater
  
  def fetchRootNode()(implicit ec: ExecutionContext): Future[KVListNode] = fetchNodeObject(rootObjectPointer) map {
    osd => KVListNode(this, KVListNodePointer(rootObjectPointer, new Array[Byte](0)), None, osd)
  }
}