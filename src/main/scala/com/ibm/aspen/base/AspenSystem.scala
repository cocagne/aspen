package com.ibm.aspen.base

import com.ibm.aspen.core.objects.ObjectPointer
import scala.concurrent.Future
import java.util.UUID
import com.ibm.aspen.core.network.Client
import com.ibm.aspen.core.read.ReadDriver

trait AspenSystem {
  
  def client: Client
  
  def readObject(pointer:ObjectPointer, readStrategy: Option[ReadDriver.Factory]): Future[ObjectStateAndData]
  
  //def getStoragePool(poolUUID: UUID): Future[StoragePool]
  
}