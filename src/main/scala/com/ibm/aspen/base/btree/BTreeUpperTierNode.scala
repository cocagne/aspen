package com.ibm.aspen.base.btree

import scala.concurrent.Future
import scala.concurrent.Promise
import scala.util.Failure
import scala.util.Success
import scala.concurrent.ExecutionContext
import scala.Left
import scala.Right


trait BTreeUpperTierNode[Key <: Ordered[Key], Value] extends BTreeNode[Key,Value] {
  import BTreeNode._
  
  // use scala.util.Sorting to create the sorted array
  val sortedLowerTierNodes: Array[NodePointer[Key]]
  
  def fetchUpperTierNode(pointer: NodePointer[Key])(implicit ec: ExecutionContext): Future[BTreeUpperTierNode[Key,Value]]
  
  def fetchLeafNode(pointer: NodePointer[Key])(implicit ec: ExecutionContext): Future[BTreeLeafNode[Key,Value]]
  
  def fetchNextNode()(implicit ec: ExecutionContext): Future[Option[BTreeUpperTierNode[Key,Value]]] = {
    nextNode match {
      case None => Future.successful(None)
      case Some(ptr) => fetchUpperTierNode(ptr).map(Some(_))
    }
  }
  
  /** Call this method only *after* checking to ensure that the key is not less than the current minimum */
  protected def scanRight(key: Key, p:Promise[BTreeUpperTierNode[Key,Value]])(implicit ec: ExecutionContext): Unit = {
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
  
  def scanToWithinRange(key: Key)(implicit ec: ExecutionContext): Future[BTreeUpperTierNode[Key,Value]] = {
    val p = Promise[BTreeUpperTierNode[Key,Value]]()
    
    if (key < minimum)
      p.failure(new KeyOutOfRange)
    else 
      scanRight(key, p)  
    
    p.future
  }
  
  /** Throws UnreachableKey if no lower node can be found for the requested key
   * 
   */
  def fetchLowerNode(key: Key, blacklisted: Set[NodePointer[Key]])(implicit ec: ExecutionContext): Future[Either[BTreeUpperTierNode[Key,Value], BTreeLeafNode[Key,Value]]] = {
    scanToWithinRange(key) flatMap (node => {
      var i = 0
      var npIndex = -1
      
      while (i < node.sortedLowerTierNodes.length && (npIndex == -1 || key > node.sortedLowerTierNodes(npIndex).minimum)) {
        if (!blacklisted.contains(node.sortedLowerTierNodes(i)))
          npIndex = i
        i += 1
      }
      
      if (npIndex == -1)
        throw new UnreachableKey
      
      val np = node.sortedLowerTierNodes(npIndex)
      
      if (tier == 1)
        node.fetchLeafNode(np) map (Right(_))
      else
        node.fetchUpperTierNode(np) map (Left(_))   
    })
  }
  
  protected def doFetch(key: Key, p: Promise[Option[Value]], path:List[BTreeUpperTierNode[Key,Value]], blacklisted: Set[NodePointer[Key]])(implicit ec: ExecutionContext): Unit = {
    fetchLowerNode(key, blacklisted)  onComplete {
      case Failure(err) => err match {
        
        case e: NodeNotFound[Key] =>
          // Probably the result of a join operation in the tier below us. Blacklist the node and recursively try again
          doFetch(key, p, path, blacklisted + e.nodePointer)
          
        case e: UnreachableKey =>
          // None of the lower pointers in this node may be used. 
          
          if (path.isEmpty) 
            p.failure(e) // No recourse. Propagate the error to the user
          else 
            path.head.doFetch(key, p, path.tail, blacklisted + nodePointer) // Blacklist this node and resume the search from our parent node
          
        case _ => p.failure(err)
      }
      
      case Success(either) => either match {
        case Left(upper) => upper.doFetch(key, p, this :: path, blacklisted)
        case Right(leaf) => leaf.fetchValue(key) onComplete {
          case Failure(err) => p.failure(err)
          case Success(ovalue) => p.success(ovalue)
        }
      }
    }
  } 
  
  def fetch(key: Key)(implicit ec: ExecutionContext): Future[Option[Value]] = {
    val p = Promise[Option[Value]]
    doFetch(key, p, Nil, Set())
    p.future
  }
}