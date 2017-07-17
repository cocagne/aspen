package com.ibm.aspen.base

import com.ibm.aspen.core.objects.ObjectPointer
import scala.concurrent.Future
import java.util.UUID

trait AspenSystem {
  
  def readObject(pointer:ObjectPointer): Future[AspenObject]
  
  def getStoragePool(poolUUID: UUID): Future[StoragePool]
  
}