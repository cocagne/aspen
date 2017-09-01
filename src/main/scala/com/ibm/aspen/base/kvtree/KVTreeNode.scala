package com.ibm.aspen.base.kvtree

import scala.collection.immutable.SortedMap
import scala.concurrent.ExecutionContext
import scala.concurrent.Promise
import scala.concurrent.Future
import scala.util.Failure
import scala.util.Success

private[kvtree] trait KVTreeNode {
  val tree: KVTree
  val tier: Int
  val nodePointer: KVTreeNodePointer
  val leftNode: Option[KVTreeNodePointer] // This is just a guess. Its primary goal is to serve as a staring point for searching for the real previous node
  val rightNode: Option[KVTreeNodePointer]
  
  def minimum = nodePointer.minimum
  
  def fetchRightNode()(implicit ec: ExecutionContext): Future[Option[KVTreeNode]]
  
  def keyWithinRange(key: Array[Byte]): Boolean = {
    val rok = rightNode match { 
      case None => true
      case Some(next) => tree.compareKeys(key, next.minimum) < 0
    }
    
    tree.compareKeys(key, nodePointer.minimum) >= 0 && rok 
  }
  
  
}

private[kvtree] object KVTreeNode {
   
  def scanToWithinRange[T <: KVTreeNode](sourceNode: T, key: Array[Byte])(implicit ec: ExecutionContext): Future[T] = {
    val p = Promise[T]()
    
    def scanRight(node: T, key: Array[Byte])(implicit ec: ExecutionContext): Unit = {
      if (node.keyWithinRange(key))
        p.success(node)
      else {
        node.fetchRightNode() onComplete {
          case Failure(err) => p.failure(err)
          case Success(onode) => onode match {
            case None => p.success(node)
            case Some(rnode) => scanRight(rnode.asInstanceOf[T], key)
          }
        }
      }
    }
    
    if (sourceNode.tree.compareKeys(key, sourceNode.minimum) < 0)
      p.failure(new KeyOutOfRange)
    else 
      scanRight(sourceNode, key)  
    
    p.future
  }  
}

