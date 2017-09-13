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

class AspenSystemImpl(
    val clientMessenger: ClientSideReadMessenger,
    val defaultReadDriverFactory: ReadDriver.Factory)(implicit ec: ExecutionContext) extends AspenSystem {
  
  import AspenSystemImpl._
  
  val readManager = new ClientReadManager(clientMessenger)
  
  def client = clientMessenger.client
  
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
          
  def newTransaction(): Transaction = null
  
  def allocateObject(
      allocInto: ObjectPointer,
      allocIntoRevision: ObjectRevision,
      poolUUID: UUID, 
      minimumSize: Int, 
      initialContent: ByteBuffer)(implicit t: Transaction, ec: ExecutionContext): Future[ObjectPointer] = null
  
  //def getStoragePool(poolUUID: UUID): Future[StoragePool]
}

object AspenSystemImpl {
  
}