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

class ClientAllocationManager(
    val clientMessenger: ClientSideAllocationMessenger,
    val driverFactory: AllocationDriver.Factory)(implicit ec: ExecutionContext) extends ClientSideAllocationMessageReceiver {
  
  private[this] var outstandingAllocations = Map[UUID, AllocationDriver]()
  
  def receive(m: AllocateResponse): Unit = { 
    synchronized { outstandingAllocations.get(m.allocationTransactionUUID) } foreach {
      driver => driver.receiveAllocationResult(m.fromStoreId, m.allocationTransactionUUID, m.result)
    }  
  }
  
  def allocateDataObject(
      messenger: ClientSideAllocationMessenger,
      transaction: Transaction,
      pool: StoragePool,
      objectSize: Option[Int],
      objectIDA: IDA,
      initialContent: DataBuffer,
      afterTimestamp: Option[HLCTimestamp],
      initialRefcount: ObjectRefcount,
      allocatingObject: ObjectPointer,
      allocatingObjectRevision: ObjectRevision): Future[Either[Map[Byte,AllocationErrors.Value], DataObjectPointer]] = {
    
    val hosts = pool.selectStoresForAllocation(objectIDA)
    val encoded = objectIDA.encode(initialContent)
    val objectData = hosts.map(_.asInstanceOf[Byte]).zip(encoded).toMap
    val newObjectUUID = UUID.randomUUID()
    val timestamp = afterTimestamp match {
      case None => HLCTimestamp.now
      case Some(ts) => HLCTimestamp.happensAfter(ts)
    }
    val options = new DataAllocationOptions
    
    val driver = driverFactory.create(clientMessenger, pool.uuid, newObjectUUID, objectSize, objectIDA, objectData, options, timestamp, 
                                      initialRefcount, transaction.uuid, allocatingObject, allocatingObjectRevision)
                                      
    synchronized { outstandingAllocations += (transaction.uuid -> driver) }
    
    driver.futureResult onComplete {
      case _ => synchronized { outstandingAllocations -= transaction.uuid }
    }
    
    driver.start()
    
    driver.futureResult map { eresult => eresult match {
      case Left(err) => Left(err)
      case Right(newObjectPtr) => 
        AllocationFinalizationAction.addToAllocationTree(transaction, pool.poolDefinitionPointer, newObjectPtr)
        Right(newObjectPtr.asInstanceOf[DataObjectPointer])
    }}
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
      initialContent: Map[Array[Byte], Array[Byte]],
      minimum: Option[Array[Byte]],
      maximum: Option[Array[Byte]],
      left: Option[Array[Byte]],
      right: Option[Array[Byte]],
      useRevisions: Boolean,
      useTimestamps: Boolean,
      useRefcounts: Boolean): Future[Either[Map[Byte,AllocationErrors.Value], KeyValueObjectPointer]] = {
    Future.failed(new Exception("TODO"))
    /*
    val hosts = pool.selectStoresForAllocation(objectIDA)
    val encoded = objectIDA.encode(initialContent)
    val objectData = hosts.map(_.asInstanceOf[Byte]).zip(encoded).toMap
    val newObjectUUID = UUID.randomUUID()
    val timestamp = afterTimestamp match {
      case None => HLCTimestamp.now
      case Some(ts) => HLCTimestamp.happensAfter(ts)
    }
    val options = new KeyValueAllocationOptions(useRevisions, useRefcounts, useTimestamps)
    
    val driver = driverFactory.create(clientMessenger, pool.uuid, newObjectUUID, objectSize, objectIDA, objectData, options, timestamp, 
                                      initialRefcount, transaction.uuid, allocatingObject, allocatingObjectRevision)
                                      
    synchronized { outstandingAllocations += (transaction.uuid -> driver) }
    
    driver.futureResult onComplete {
      case _ => synchronized { outstandingAllocations -= transaction.uuid }
    }
    
    driver.start()
    
    driver.futureResult map { eresult => eresult match {
      case Left(err) => Left(err)
      case Right(newObjectPtr) => 
        AllocationFinalizationAction.addToAllocationTree(transaction, pool.poolDefinitionPointer, newObjectPtr)
        Right(newObjectPtr.asInstanceOf[DataObjectPointer])
    }}
    */
  }
}