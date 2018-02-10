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
import com.ibm.aspen.core.objects.DataObjectPointer
import com.ibm.aspen.core.objects.KeyValueObjectPointer
import com.ibm.aspen.core.objects.DataObjectState
import com.ibm.aspen.core.objects.KeyValueObjectState
import com.ibm.aspen.core.objects.keyvalue.KeyValueOperation
import com.ibm.aspen.core.transaction.KeyValueUpdate
import com.ibm.aspen.core.objects.keyvalue.Key
import com.ibm.aspen.core.objects.keyvalue.KeyOrdering

class SimpleTestSystem extends AspenSystem {
  val poolUUID = new UUID(0,0)
  
  val now = HLCTimestamp.now
  
  def shutdown(): Unit = ()
  
  def createTaskGroup(groupUUID: UUID, taskGroupType: UUID, groupDefinitionContent: DataBuffer): Future[TaskGroup] = Future.failed(new Exception("NOT SUPPORTED IN SIMPLE TEST SYSTEM"))
  def getTaskGroup(groupUUID: UUID): Future[TaskGroup] = Future.failed(new Exception("NOT SUPPORTED IN SIMPLE TEST SYSTEM"))
  def createTaskGroupExecutor(groupUUID: UUID): Future[TaskGroupExecutor] = Future.failed(new Exception("NOT SUPPORTED IN SIMPLE TEST SYSTEM"))
  
  def mkptr(objectNum:Int) = DataObjectPointer(new UUID(0,objectNum), poolUUID, None, Replication(3,2), new Array[StorePointer](0)) 
 
  class Obj(var rev: ObjectRevision, var ref: ObjectRefcount, var data: DataBuffer, var timestamp: HLCTimestamp)
  
  var content = Map[UUID, Obj]()
  var allocCount = 0
  
  def getStoragePool(poolUUID: UUID): Future[StoragePool] = Future.failed(new NotImplementedError)
  def getStoragePool(storagePoolDefinitionPointer: DataObjectPointer): Future[StoragePool] = Future.failed(new NotImplementedError)
  
  def clientId: ClientID = ClientID(poolUUID)
  
  def readObject(pointer:DataObjectPointer, readStrategy: Option[ReadDriver.Factory]): Future[DataObjectState] = {
    content.get(pointer.uuid) match {
      case Some(o) => Future.successful(DataObjectState(pointer, o.rev, o.ref, o.timestamp, 0, o.data))
      case None => Future.failed(new DataRetrievalFailed)
    }
  }
  def readObject(pointer:KeyValueObjectPointer, readStrategy: Option[ReadDriver.Factory]): Future[KeyValueObjectState] = Future.failed(new Exception("not supported"))
  
  def readSingleKey(pointer: KeyValueObjectPointer, key: Key, comparison: KeyOrdering): Future[KeyValueObjectState] = Future.failed(new Exception("not supported"))
  
  def readLargestKeyLessThan(pointer: KeyValueObjectPointer, key: Key, comparison: KeyOrdering): Future[KeyValueObjectState] = Future.failed(new Exception("not supported"))
  
  def readLargestKeyLessThanOrEqualTo(pointer: KeyValueObjectPointer, key: Key, comparison: KeyOrdering): Future[KeyValueObjectState] = Future.failed(new Exception("not supported"))
  
  def readKeyRange(pointer: KeyValueObjectPointer, minimum: Key, maximum: Key, comparison: KeyOrdering): Future[KeyValueObjectState] = Future.failed(new Exception("not supported"))
  
  def newTransaction(): Transaction = new Tx
  
  def lowLevelAllocateDataObject(
      allocInto: ObjectPointer,
      allocIntoRevision: ObjectRevision,
      poolUUID: UUID,
      objectSize: Option[Int],
      objectIDA: IDA,
      initialContent: DataBuffer,
      timestamp: Option[HLCTimestamp])(implicit t: Transaction, ec: ExecutionContext): Future[DataObjectPointer] = {
    
    val id = allocCount
    allocCount += 1
    val ptr = mkptr(id)
    
    content += (ptr.uuid -> new Obj(t.txRevision, ObjectRefcount(0,1), initialContent, now))
    
    Future.successful(ptr)
  }
  def lowLevelAllocateKeyValueObject(
      allocatingObject: ObjectPointer,
      allocatingObjectRevision: ObjectRevision,
      poolUUID: UUID,
      objectSize: Option[Int],
      objectIDA: IDA,
      initialContent: List[KeyValueOperation],
      afterTimestamp: Option[HLCTimestamp] = None)(implicit t: Transaction, ec: ExecutionContext): Future[KeyValueObjectPointer] = {
    Future.failed(new Exception("Not supported"))
  }
  
  class Tx extends Transaction {
    val uuid = UUID.randomUUID()
    
    val p = Promise[Unit]()
    
    val result = p.future
    
    var ops = List[ ()=>Unit ]()
    
    var fas = Map[UUID, Array[Byte]]()
    
    var invalidatedReason: Option[Throwable] = None
    
    var notifyOnResolution = Set[DataStoreID]()
    
    var happensAfter: Option[HLCTimestamp] = None
    
    override def append(objectPointer: DataObjectPointer, requiredRevision: ObjectRevision, data: DataBuffer): ObjectRevision = {
      
      def fn() = {
        val o = content(objectPointer.uuid) 
        o.rev = txRevision
        val cpy = ByteBuffer.allocate(o.data.size + data.size)
        cpy.put(o.data.asReadOnlyBuffer())
        cpy.put(data.asReadOnlyBuffer())
        cpy.position(0)
        o.data = DataBuffer(cpy)
        ()
      }
      ops = fn _ :: ops
      txRevision
    }
    
    override def overwrite(objectPointer: DataObjectPointer, requiredRevision: ObjectRevision, data: DataBuffer): ObjectRevision =  {
      
      def fn() = {
        val o = content(objectPointer.uuid) 
        o.rev = txRevision
        val cpy = ByteBuffer.allocate(data.size)
        cpy.put(data.asReadOnlyBuffer())
        cpy.position(0)
        o.data = DataBuffer(cpy)
        ()
      }
      ops = fn _ :: ops
      txRevision
    }
    
    def append(
      pointer: KeyValueObjectPointer, 
      requiredRevision: Option[ObjectRevision],
      requirements: List[KeyValueUpdate.KVRequirement],
      operations: List[KeyValueOperation]): Unit = ()
      
    def overwrite(
      pointer: KeyValueObjectPointer, 
      requiredRevision: ObjectRevision,
      requirements: List[KeyValueUpdate.KVRequirement],
      operations: List[KeyValueOperation]): Unit = ()
    
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
        o.rev = txRevision
        ()
      }
      ops = fn _ :: ops
      txRevision
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