package com.ibm.aspen.base.btree

import scala.concurrent.Future
import com.ibm.aspen.core.objects.ObjectPointer
import scala.concurrent.ExecutionContext
import com.ibm.aspen.base.AspenSystem

trait BTreeNode[Key <: Ordered[Key], Value] {
  import BTreeNode._
  
  val treeRoot: NodePointer[Key]
  val tier: Int
  val nodePointer: NodePointer[Key]
  val previousNode: Option[NodePointer[Key]] // This is just a guess. Its primary goal is to serve as a staring point for searching for the real previous node
  val nextNode: Option[NodePointer[Key]]
  def system: AspenSystem
  
  def minimum = nodePointer.minimum
  
  def keyWithinRange(key: Key): Boolean = {
    val rok = nextNode match { 
      case None => true
      case Some(next) => key < next.minimum
    }
    
    key >= nodePointer.minimum && rok 
  }
      
  /** Fetches a value from the tree */
  def fetch(key: Key)(implicit ec: ExecutionContext): Future[Option[Value]]
  
  /** Fetches the leaf node with the key range including the specified key */
  def fetchLeafNode(key: Key)(implicit ec: ExecutionContext): Future[BTreeLeafNode[Key,Value]]
}

object BTreeNode {
  case class NodePointer[Key <: Ordered[Key]](objectPointer:ObjectPointer, minimum: Key) extends Ordered[NodePointer[Key]] {
    def compare(that: NodePointer[Key]) = {
      val keyComp = minimum.compare(that.minimum)
      
      if (keyComp == 0) 
        objectPointer.uuid.compareTo(that.objectPointer.uuid)
      else
        keyComp
    }
  }
}