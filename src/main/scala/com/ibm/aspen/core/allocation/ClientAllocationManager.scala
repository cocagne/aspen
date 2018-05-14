package com.ibm.aspen.core.allocation

import com.ibm.aspen.core.network.ClientSideAllocationMessenger
import scala.concurrent.ExecutionContext
import java.util.UUID
import com.ibm.aspen.core.ida.IDA
import com.ibm.aspen.core.objects.ObjectRefcount
import com.ibm.aspen.core.objects.ObjectPointer
import com.ibm.aspen.core.objects.ObjectRevision
import scala.concurrent.Future
import java.nio.ByteBuffer
import com.ibm.aspen.core.network.ClientSideAllocationMessageReceiver
import com.ibm.aspen.core.DataBuffer
import com.ibm.aspen.core.HLCTimestamp
import com.ibm.aspen.core.objects.DataObjectPointer
import com.ibm.aspen.base.Transaction
import com.ibm.aspen.base.StoragePool
import com.ibm.aspen.base.impl.AllocationFinalizationAction
import com.ibm.aspen.core.objects.KeyValueObjectPointer
import com.ibm.aspen.core.objects.keyvalue.KeyValueOperation
import com.ibm.aspen.core.objects.keyvalue.SetMin
import com.ibm.aspen.core.objects.keyvalue.SetMax
import com.ibm.aspen.core.objects.keyvalue.SetLeft
import com.ibm.aspen.core.objects.keyvalue.SetRight
import com.ibm.aspen.core.objects.keyvalue.Insert
import com.ibm.aspen.core.objects.keyvalue.KeyValueObjectCodec
import com.ibm.aspen.core.objects.keyvalue.Key

class ClientAllocationManager(
    val clientMessenger: ClientSideAllocationMessenger,
    val driverFactory: AllocationDriver.Factory)(implicit ec: ExecutionContext) extends ClientSideAllocationMessageReceiver {
  
  // Maps newObjectUUID -> driver
  private[this] var outstandingAllocations = Map[UUID, AllocationDriver]()
  
  def shutdown(): Unit = outstandingAllocations.foreach( t => t._2.shutdown() )
  
  def receive(m: AllocateResponse): Unit = { 
    synchronized { outstandingAllocations.get(m.newObjectUUID) } foreach {
      driver => driver.receiveAllocationResult(m.fromStoreId, m.result)
    }  
  }
  
  private def allocate[PointerType <: ObjectPointer](
      messenger: ClientSideAllocationMessenger,
      transaction: Transaction,
      pool: StoragePool,
      objectSize: Option[Int],
      objectIDA: IDA,
      encodedContent: Array[DataBuffer],
      timestamp: HLCTimestamp,
      options: AllocationOptions,
      initialRefcount: ObjectRefcount,
      allocatingObject: ObjectPointer,
      allocatingObjectRevision: ObjectRevision,
      ): Future[Either[Map[Byte,AllocationErrors.Value], PointerType]] = {
    
    val hosts = pool.selectStoresForAllocation(objectIDA)
    val objectData = hosts.map(_.asInstanceOf[Byte]).zip(encodedContent).toMap
    val newObjectUUID = UUID.randomUUID()
    
    val driver = driverFactory.create(clientMessenger, pool.uuid, newObjectUUID, objectSize, objectIDA, objectData, options, timestamp, 
                                      initialRefcount, transaction.uuid, allocatingObject, allocatingObjectRevision)
                                      
    synchronized { outstandingAllocations += (newObjectUUID -> driver) }
    
    driver.futureResult onComplete {
      case _ => synchronized { outstandingAllocations -= newObjectUUID }
    }
    
    val r = driver.futureResult map { eresult => eresult match {
      case Left(err) => Left(err)
      case Right(newObjectPtr) => 
        AllocationFinalizationAction.addToAllocationTree(transaction, pool.poolDefinitionPointer, newObjectPtr)
        Right(newObjectPtr.asInstanceOf[PointerType])
    }}
    
    driver.start()
    
    r
  }
  
  def allocateDataObject(
      messenger: ClientSideAllocationMessenger,
      transaction: Transaction,
      pool: StoragePool,
      objectSize: Option[Int],
      objectIDA: IDA,
      encodedContent: Array[DataBuffer],
      afterTimestamp: Option[HLCTimestamp],
      initialRefcount: ObjectRefcount,
      allocatingObject: ObjectPointer,
      allocatingObjectRevision: ObjectRevision): Future[Either[Map[Byte,AllocationErrors.Value], DataObjectPointer]] = {

    val timestamp = afterTimestamp match {
      case None => HLCTimestamp.now
      case Some(ts) => HLCTimestamp.happensAfter(ts)
    }
    
    val options = new DataAllocationOptions
    
    allocate(messenger, transaction, pool, objectSize, objectIDA, encodedContent, timestamp, options, 
        initialRefcount, allocatingObject, allocatingObjectRevision)
  }
  
  def allocateKeyValueObject(
      messenger: ClientSideAllocationMessenger,
      transaction: Transaction,
      pool: StoragePool,
      objectSize: Option[Int],
      objectIDA: IDA,
      afterTimestamp: Option[HLCTimestamp],
      initialRefcount: ObjectRefcount,
      allocatingObject: ObjectPointer,
      allocatingObjectRevision: ObjectRevision,
      encodedContent: Array[DataBuffer]): Future[Either[Map[Byte,AllocationErrors.Value], KeyValueObjectPointer]] = {
    
    val timestamp = afterTimestamp match {
      case None => HLCTimestamp.now
      case Some(ts) => HLCTimestamp.happensAfter(ts)
    }
    
    val options = new KeyValueAllocationOptions

    allocate(messenger, transaction, pool, objectSize, objectIDA, encodedContent, timestamp, options, 
        initialRefcount, allocatingObject, allocatingObjectRevision)
  }
}