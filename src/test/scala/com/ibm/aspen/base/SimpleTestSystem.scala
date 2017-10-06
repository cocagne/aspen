package com.ibm.aspen.base

import com.ibm.aspen.core.objects.ObjectPointer
import java.util.UUID
import com.ibm.aspen.core.ida.Replication
import com.ibm.aspen.core.objects.StorePointer
import java.nio.ByteBuffer
import scala.collection.immutable.SortedMap
import com.ibm.aspen.core.objects.ObjectRevision
import com.ibm.aspen.core.objects.ObjectRefcount
import scala.concurrent.Promise
import scala.concurrent.Future
import scala.concurrent._
import scala.concurrent.duration._
import ExecutionContext.Implicits.global
import com.ibm.aspen.core.network.ClientID
import com.ibm.aspen.core.read.ReadDriver
import com.ibm.aspen.core.read.DataRetrievalFailed
import com.ibm.aspen.core.ida.IDA

class SimpleTestSystem extends AspenSystem {
  val poolUUID = new UUID(0,0)
  
  def mkptr(objectNum:Int) = ObjectPointer(new UUID(0,objectNum), poolUUID, None, Replication(3,2), new Array[StorePointer](0)) 
 
  class Obj(var rev: ObjectRevision, var ref: ObjectRefcount, var data: ByteBuffer)
  
  var content = Map[UUID, Obj]()
  var allocCount = 0
  
  def client: ClientID = ClientID(poolUUID)
  
  def readObject(pointer:ObjectPointer, readStrategy: Option[ReadDriver.Factory]): Future[ObjectStateAndData] = {
    content.get(pointer.uuid) match {
      case Some(o) => Future.successful(ObjectStateAndData(pointer, o.rev, o.ref, o.data.asReadOnlyBuffer()))
      case None => Future.failed(new DataRetrievalFailed)
    }
  }
  
  def newTransaction(): Transaction = new Tx
  
  def allocateObject(
      allocInto: ObjectPointer,
      allocIntoRevision: ObjectRevision,
      poolUUID: UUID,
      objectSize: Option[Int],
      objectIDA: IDA,
      initialContent: ByteBuffer)(implicit t: Transaction, ec: ExecutionContext): Future[ObjectPointer] = {
    
    val id = allocCount
    allocCount += 1
    val ptr = mkptr(id)
    val len = initialContent.limit - initialContent.position
    
    content += (ptr.uuid -> new Obj(ObjectRevision(0,len), ObjectRefcount(0,1), initialContent))
    
    Future.successful(ptr)
  }
  
  class Tx extends Transaction {
    val uuid = new UUID(0,0)
    
    val p = Promise[Unit]()
    
    val result = p.future
    
    var ops = List[ ()=>Unit ]()
    
    var fas = Map[UUID, Array[Byte]]()
    
    var invalidatedReason: Option[Throwable] = None
    
    override def append(objectPointer: ObjectPointer, requiredRevision: ObjectRevision, data: ByteBuffer): ObjectRevision = {
      val len = data.limit - data.position
      
      def fn() = {
        val o = content(objectPointer.uuid) 
        o.rev = requiredRevision.append(len)
        val orig_data = o.data
        o.data = ByteBuffer.allocate(o.data.capacity + len)
        o.data.put(orig_data)
        o.data.put(data)
        o.data.position(0)
        ()
      }
      ops = fn _ :: ops
      requiredRevision.append(len)
    }
    
    override def overwrite(objectPointer: ObjectPointer, requiredRevision: ObjectRevision, data: ByteBuffer): ObjectRevision =  {
      val len = data.limit - data.position
      
      def fn() = {
        val o = content(objectPointer.uuid) 
        o.rev = requiredRevision.overwrite(len)
        o.data = ByteBuffer.allocate(len)
        o.data.put(data)
        o.data.position(0)
        ()
      }
      ops = fn _ :: ops
      requiredRevision.overwrite(len)
    }
    
    override def setRefcount(objectPointer: ObjectPointer, requiredRefcount: ObjectRefcount, refcount: ObjectRefcount): ObjectRefcount = {
      def fn() = {
        val o = content(objectPointer.uuid) 
        o.ref = refcount
        ()
      }
      ops = fn _ :: ops
      refcount
    }
    
    def invalidateTransaction(reason: Throwable): Unit = invalidatedReason = Some(reason)
    
    def addFinalizationAction(finalizationActionUUID: UUID, serializedContent: Array[Byte]): Unit = fas += (finalizationActionUUID -> serializedContent)
    
    def commit(): Future[Unit] = {
      ops.foreach(fn => fn())
      p.success(())
      p.future
    }
  }

}