package com.ibm.aspen.base.btree

import scala.concurrent._
import scala.concurrent.duration._
import ExecutionContext.Implicits.global
import org.scalatest._
import scala.util.Success
import scala.util.Failure
import java.util.UUID
import com.ibm.aspen.core.objects.ObjectPointer
import com.ibm.aspen.core.ida.Replication
import com.ibm.aspen.core.objects.StorePointer
import scala.collection.immutable.SortedMap


object BTreeSuite {
  val awaitDuration = Duration(100, MILLISECONDS)
  val poolUUID = new UUID(0,0)
  
  import BTreeNode.NodePointer
  
  case class IKey(val key:Int) extends Ordered[IKey] {
    def compare(that: IKey) = key - that.key
  }
  
  def mkptr(objectNum:Int) = ObjectPointer(new UUID(0,objectNum), poolUUID, None, Replication(3,2), new Array[StorePointer](0)) 
  
  def mknp(minimum:Int) = NodePointer[IKey](mkptr(minimum), IKey(minimum))
  
  class Upper(
      minimum: Int,
      val tier:Int,
      val next: Option[Upper],
      contents: List[Either[Upper, Leaf]]) extends BTreeNode[IKey,Int] with BTreeUpperTierNode[IKey,Int] {
    
    val nodePointer:NodePointer[IKey] = mknp(minimum)
    val nextNode: Option[NodePointer[IKey]] = next.map(_.nodePointer)
    
    var sm = SortedMap[NodePointer[IKey], Either[Upper,Leaf]]()
    
    contents.foreach(e => e match{
      case Left(upper) => sm += (upper.nodePointer -> Left(upper))
      case Right(leaf) => sm += (leaf.nodePointer -> Right(leaf))
    }) 
    
    val sortedLowerTierNodes: Array[NodePointer[IKey]] = sm.keysIterator.toArray
    
    def fetchUpperTierNode(pointer: NodePointer[IKey])(implicit ec: ExecutionContext): Future[BTreeUpperTierNode[IKey,Int]] = sm.get(pointer) match {
      case None => next match {
        case None => Future.failed(new NodeNotFound)
        case Some(r) => if (r.nodePointer == pointer) Future.successful(r) else Future.failed(new NodeNotFound) 
      }
      case Some(either) => either match {
        case Left(upper) => Future.successful(upper)
        case Right(leaf) => Future.failed(new Exception("leaf found where upper expected"))
      }
    }
  
    def fetchLeafNode(pointer: NodePointer[IKey])(implicit ec: ExecutionContext): Future[BTreeLeafNode[IKey,Int]] = sm.get(pointer) match {
      case None => println(s"POINTER $pointer sm: $sm")
      Future.failed(new NodeNotFound)
      case Some(either) => either match {
        case Left(upper) => Future.failed(new Exception("upper found where leaf expected"))
        case Right(leaf) => Future.successful(leaf)
      }
    }
  }
      
  class Leaf(
      minimum: Int,
      val next: Option[Leaf],
      contents:List[Int]) extends BTreeNode[IKey,Int] with BTreeLeafNode[IKey,Int] {
    
    val nodePointer:NodePointer[IKey] = mknp(minimum)
    var sm = SortedMap[IKey,Int]()
    
    contents.foreach(i => sm += (IKey(i) -> i))
    
    val nextNode: Option[NodePointer[IKey]] = next.map(_.nodePointer)
    
    def getValueFromThisNode(key: IKey): Option[Int] = sm.get(key) 
    
    def fetchLeafNode(pointer: NodePointer[IKey])(implicit ec: ExecutionContext): Future[BTreeLeafNode[IKey,Int]] = next match {
      case None => Future.failed(new NodeNotFound)
      case Some(leaf) => Future.successful(leaf)
    }
  }
}

class BTreeSuite extends AsyncFunSuite with Matchers {
  // Await.result(ds.getCurrentObjectState(txd), awaitDuration)
  import BTreeSuite._
  
  test("Simple leaf fetch") {
    val l = new Leaf(0, None, List(1,2,3))
    l.fetchValue(IKey(2)) map { v => v should be (Some(2)) }
  }
  
  test("Simple leaf fetch with missing value") {
    val l = new Leaf(0, None, List(1,2,3))
    l.fetchValue(IKey(4)) map { v => v should be (None) }
  }
  
  test("Chained fetch") {
    val r = new Leaf(10, None, List(11,12,13))
    val l = new Leaf(0, Some(r), List(1,2,3))
    l.fetchValue(IKey(12)) map { v => v should be (Some(12)) }
  }
  
  test("Tiered fetch") {
    val l = new Leaf(0, None, List(1,2,3))
    val u = new Upper(0, 1, None, List(Right(l)))
    u.fetch(IKey(2)) map { v => v should be (Some(2)) }
  }
  
  test("Multi-tiered fetch") {
    val l = new Leaf(0, None, List(1,2,3))
    val u = new Upper(0, 1, None, List(Right(l)))
    val u2 = new Upper(0, 2, None, List(Left(u)))
    u2.fetch(IKey(2)) map { v => v should be (Some(2)) }
  }
  
  test("Multi-tiered with navigation fetch") {
    val l = new Leaf(5, None, List(5,6,7))
    val u = new Upper(4, 1, None, List(Right(l)))
    val u2 = new Upper(3, 1, Some(u), List())
    val u3 = new Upper(0, 2, None, List(Left(u), Left(u2)))
    u3.fetch(IKey(5)) map { v => v should be (Some(5)) }
  }
  
  test("Multi-tiered and chained fetch") {
    val l = new Leaf(5, None, List(5,6,7))
    val u = new Upper(4, 1, None, List(Right(l)))
    val u2 = new Upper(3, 1, Some(u), List())
    u2.fetch(IKey(5)) map { v => v should be (Some(5)) }
  }
}