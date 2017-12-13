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
import com.ibm.aspen.core.DataBuffer
import com.ibm.aspen.core.data_store.DataStoreID
import com.ibm.aspen.core.HLCTimestamp

class SimpleTestSystem extends AspenSystem {
  val poolUUID = new UUID(0,0)
  
  val now = HLCTimestamp.now
  
  def mkptr(objectNum:Int) = ObjectPointer(new UUID(0,objectNum), poolUUID, None, Replication(3,2), new Array[StorePointer](0)) 
 
  class Obj(var rev: ObjectRevision, var ref: ObjectRefcount, var data: DataBuffer, var timestamp: HLCTimestamp)
  
  var content = Map[UUID, Obj]()
  var allocCount = 0
  
  def getStoragePool(poolUUID: UUID): Future[StoragePool] = Future.failed(new NotImplementedError)
  def getStoragePool(storagePoolDefinitionPointer: ObjectPointer): Future[StoragePool] = Future.failed(new NotImplementedError)
  
  def clientId: ClientID = ClientID(poolUUID)
  
  def readObject(pointer:ObjectPointer, readStrategy: Option[ReadDriver.Factory]): Future[ObjectStateAndData] = {
    content.get(pointer.uuid) match {
      case Some(o) => Future.successful(ObjectStateAndData(pointer, o.rev, o.ref, o.timestamp, o.data))
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
      initialContent: DataBuffer,
      timestamp: Option[HLCTimestamp])(implicit t: Transaction, ec: ExecutionContext): Future[ObjectPointer] = {
    
    val id = allocCount
    allocCount += 1
    val ptr = mkptr(id)
    
    content += (ptr.uuid -> new Obj(ObjectRevision(0,initialContent.size), ObjectRefcount(0,1), initialContent, now))
    
    Future.successful(ptr)
  }
  
  class Tx extends Transaction {
    val uuid = new UUID(0,0)
    
    val p = Promise[Unit]()
    
    val result = p.future
    
    var ops = List[ ()=>Unit ]()
    
    var fas = Map[UUID, Array[Byte]]()
    
    var invalidatedReason: Option[Throwable] = None
    
    var notifyOnResolution = Set[DataStoreID]()
    
    var happensAfter: Option[HLCTimestamp] = None
    
    override def append(objectPointer: ObjectPointer, requiredRevision: ObjectRevision, data: DataBuffer): ObjectRevision = {
      
      def fn() = {
        val o = content(objectPointer.uuid) 
        o.rev = requiredRevision.append(data.size)
        val cpy = ByteBuffer.allocate(o.data.size + data.size)
        cpy.put(o.data.asReadOnlyBuffer())
        cpy.put(data.asReadOnlyBuffer())
        cpy.position(0)
        o.data = DataBuffer(cpy)
        ()
      }
      ops = fn _ :: ops
      requiredRevision.append(data.size)
    }
    
    override def overwrite(objectPointer: ObjectPointer, requiredRevision: ObjectRevision, data: DataBuffer): ObjectRevision =  {
      
      def fn() = {
        val o = content(objectPointer.uuid) 
        o.rev = requiredRevision.overwrite(data.size)
        val cpy = ByteBuffer.allocate(data.size)
        cpy.put(data.asReadOnlyBuffer())
        cpy.position(0)
        o.data = DataBuffer(cpy)
        ()
      }
      ops = fn _ :: ops
      requiredRevision.overwrite(data.size)
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
    
    def bumpVersion(objectPointer: ObjectPointer, requiredRevision: ObjectRevision): ObjectRevision = {
      def fn() = {
        val o = content(objectPointer.uuid) 
        o.rev = requiredRevision.versionBump()
        ()
      }
      ops = fn _ :: ops
      requiredRevision.versionBump()
    }
    
    def ensureHappensAfter(timestamp: HLCTimestamp): Unit = synchronized {
      happensAfter match {
        case Some(ts) => if (timestamp.compareTo(ts) > 0) happensAfter = Some(timestamp)
        case None => happensAfter = Some(timestamp)
      }
    }
    
    def timestamp(): HLCTimestamp = synchronized {
      happensAfter match {
        case Some(ts) => HLCTimestamp.happensAfter(ts)
        case None => HLCTimestamp.now
      }
    }
    
    def invalidateTransaction(reason: Throwable): Unit = invalidatedReason = Some(reason)
    
    def addFinalizationAction(finalizationActionUUID: UUID, serializedContent: Array[Byte]): Unit = fas += (finalizationActionUUID -> serializedContent)
    
    def addNotifyOnResolution(storesToNotify: Set[DataStoreID]): Unit = notifyOnResolution ++ storesToNotify
    
    def commit()(implicit ec: ExecutionContext): Future[Unit] = {
      ops.foreach(fn => fn())
      p.success(())
      p.future
    }
  }

}