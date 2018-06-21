package com.ibm.aspen.base

import com.ibm.aspen.core.data_store.DataStoreID
import java.util.UUID
import scala.concurrent.ExecutionContext
import scala.concurrent.Future

trait StorageHost {
  
  val uuid: UUID
  
  def online: Boolean
  
  def ownsStore(storeId: DataStoreID)(implicit ec: ExecutionContext): Future[Boolean]
}