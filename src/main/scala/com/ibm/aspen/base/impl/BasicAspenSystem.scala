package com.ibm.aspen.base.impl

import com.ibm.aspen.base.AspenSystem
import java.util.UUID
import com.ibm.aspen.core.network.Client
import com.ibm.aspen.core.network.ClientSideReadMessenger
import com.ibm.aspen.core.read.ClientReadManager
import scala.concurrent.ExecutionContext
import com.ibm.aspen.core.objects.ObjectPointer
import scala.concurrent.Future
import com.ibm.aspen.base.StoragePool
import com.ibm.aspen.core.read.ReadDriver
import com.ibm.aspen.base.ObjectStateAndData
import com.ibm.aspen.core.read.DataRetrievalFailed
import com.ibm.aspen.base.Transaction
import java.nio.ByteBuffer
import com.ibm.aspen.core.objects.ObjectRevision
import com.ibm.aspen.core.transaction.ClientTransactionDriver
import com.ibm.aspen.core.transaction.ClientTransactionManager
import com.ibm.aspen.core.allocation.ClientAllocationManager
import com.ibm.aspen.core.allocation.AllocationDriver
import com.ibm.aspen.core.ida.IDA

class BasicAspenSystem(
    chooseDesignatedLeader: (ObjectPointer) => Byte, // Uses peer online/offline knowledge to select designated leaders for transactions
    val messenger: ClientMessenger,
    val defaultReadDriverFactory: ReadDriver.Factory,
    val defaultTransactionDriverFactory: ClientTransactionDriver.Factory,
    val defaultAllocationDriverFactory: AllocationDriver.Factory,
    val transactionFactory: (BasicAspenSystem) => Transaction
    )(implicit ec: ExecutionContext) extends AspenSystem {
  
  import BasicAspenSystem._
  
  val readManager = new ClientReadManager(messenger)
  val txManager = new ClientTransactionManager(messenger, chooseDesignatedLeader, defaultTransactionDriverFactory)
  val allocManager = new ClientAllocationManager(messenger, defaultAllocationDriverFactory)
  
  messenger.setMessageReceivers(txManager, readManager, allocManager)
  
  def client = messenger.client
  
  def readObject(
      objectPointer:ObjectPointer, 
      readStrategy: Option[ReadDriver.Factory] ): Future[ObjectStateAndData] = readManager.read(objectPointer, true, false, 
          readStrategy.getOrElse(defaultReadDriverFactory)).map(r => r match {
            case Left(err) => throw err
            case Right(os) => os.data match {
              case Some(data) => ObjectStateAndData(objectPointer, os.revision, os.refcount, data.asReadOnlyBuffer())
              case None => throw DataRetrievalFailed()
            }
          })
          
  def newTransaction(): Transaction = transactionFactory(this)
  /* def allocate(messenger: ClientSideAllocationMessenger,
               poolUUID: UUID,
               newObjectUUID: UUID,
               objectSize: Option[Int],
               objectIDA: IDA,
               objectData: Map[Byte,ByteBuffer], // Map DataStore pool index -> store-specific ObjectData
               initialRefcount: ObjectRefcount,
               allocationTransactionUUID: UUID,
               allocatingObject: ObjectPointer,
               allocatingObjectRevision: ObjectRevision): Future[Either[Map[Byte,AllocationError.Value], ObjectPointer]]*/
  def allocateObject(
      allocInto: ObjectPointer,
      allocIntoRevision: ObjectRevision,
      poolUUID: UUID, 
      objectSize: Option[Int],
      objectIDA: IDA,
      initialContent: ByteBuffer)(implicit t: Transaction, ec: ExecutionContext): Future[ObjectPointer] = {
    // Create Tx UUID
    // Use IDA to encode the initial content buffer
    // Need the Storage Pool record to know num stores
    // Need a mechanism to select the hosting stores
    
    //allocManager.allocate(messenger, poolUUID, objectSize,
    
    Future.failed(new Exception("TODO"))
  }
  
  //def getStoragePool(poolUUID: UUID): Future[StoragePool]
}

object BasicAspenSystem {
  
}