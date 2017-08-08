package com.ibm.aspen.base.btree

import scala.concurrent.Future
import scala.collection.immutable.SortedMap
import scala.concurrent.ExecutionContext
import scala.concurrent.Promise
import scala.util.Failure
import scala.util.Success

trait BTreeLeafNode[Key <: Ordered[Key], Value] extends BTreeNode[Key,Value]  {
  
  import BTreeNode._
  
  val tier: Int = 0
  
  protected def getValueFromThisNode(key: Key): Option[Value]
  
  def fetchLeafNode(pointer: NodePointer[Key])(implicit ec: ExecutionContext): Future[BTreeLeafNode[Key,Value]]
  
  def fetchNextNode()(implicit ec: ExecutionContext): Future[Option[BTreeLeafNode[Key,Value]]] = {
    nextNode match {
      case None => Future.successful(None)
      case Some(ptr) => fetchLeafNode(ptr).map(Some(_))
    }
  }
  
  /** Call this method only *after* checking to ensure that the key is not less than the current minimum */
  protected def scanRight(key: Key, p:Promise[BTreeLeafNode[Key,Value]])(implicit ec: ExecutionContext): Unit = {
    if (keyWithinRange(key))
      p.success(this)
    else {
      fetchNextNode() onComplete {
        case Failure(err) => p.failure(err)
        case Success(onode) => onode match {
          case None => p.success(this)
          case Some(node) => node.scanRight(key, p)
        }
      }
    }
  }
  
  def scanToWithinRange(key: Key)(implicit ec: ExecutionContext): Future[BTreeLeafNode[Key,Value]] = {
    val p = Promise[BTreeLeafNode[Key,Value]]()
    
    if (key < minimum)
      p.failure(new KeyOutOfRange)
    else 
      scanRight(key, p)  
    
    p.future
  }
  
  def fetchValue(key: Key)(implicit ec: ExecutionContext): Future[Option[Value]] = {
    scanToWithinRange(key) map (node => node.getValueFromThisNode(key))
  }
  
  protected def doFetch(key: Key, p: Promise[Option[Value]])(implicit ec: ExecutionContext): Unit = {
    scanToWithinRange(key)  onComplete {
      case Failure(err) => p.failure(err)
      case Success(leaf) => leaf.fetchValue(key) onComplete {
        case Failure(err) => p.failure(err)
        case Success(ovalue) => p.success(ovalue)
      }
    }
  } 
  
  def fetch(key: Key)(implicit ec: ExecutionContext): Future[Option[Value]] = {
    val p = Promise[Option[Value]]
    doFetch(key, p)
    p.future
  }
}