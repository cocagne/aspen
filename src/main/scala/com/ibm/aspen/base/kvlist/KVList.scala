package com.ibm.aspen.base.kvlist

import com.ibm.aspen.base.AspenSystem
import scala.concurrent.ExecutionContext
import com.ibm.aspen.core.objects.ObjectPointer
import scala.concurrent.Future
import com.ibm.aspen.base.ObjectStateAndData

trait KVList {
   
  def fetchNodeObject(objectPointer: ObjectPointer): Future[ObjectStateAndData]
  
  def fetchCachedNode(objectPointer: ObjectPointer): Option[KVListNode] = None
  
  def updateCachedNode(node: KVListNode): Unit = ()
  
  def dropCachedNode(node: KVListNode): Unit = ()
  
  def fetchNode(p: KVListNodePointer)(implicit ec: ExecutionContext): Future[KVListNode] = {
      fetchCachedNode(p.objectPointer) match {
        case Some(n) => Future.successful(n)
        
        case None => fetchNodeObject(p.objectPointer) map {
          osd => KVListNode(this, p, osd)
        }
      }
  }
  
  def compareKeys(a: Array[Byte], b: Array[Byte]): Int
  
  val rootObjectPointer: ObjectPointer
  
  val objectAllocater: KVListNodeAllocater
  
  def fetchRootNode()(implicit ec: ExecutionContext): Future[KVListNode] = fetchNode(KVListNodePointer(rootObjectPointer, new Array[Byte](0)))
}

object KVList {
  class KeyOrdering(val keyCompare: (Array[Byte], Array[Byte]) => Int) extends Ordering[Array[Byte]] {
    def compare(a: Array[Byte], b: Array[Byte]) = keyCompare(a, b)
  }
}