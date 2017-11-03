package com.ibm.aspen.base.impl

import com.ibm.aspen.core.network.StoreSideTransactionMessenger
import com.ibm.aspen.core.network.StoreSideAllocationMessenger
import com.ibm.aspen.core.network.StoreSideReadMessenger
import scala.concurrent.Future
import com.ibm.aspen.core.network.ClientID

trait StorageNodeMessenger extends StoreSideTransactionMessenger with StoreSideAllocationMessenger with StoreSideReadMessenger {
  
  def client: ClientID
  
  /** Called after the storage node has done its internal initialization and is ready to begin handling messages.
   *  
   *  Returns a Future to messenger initialization complete. The StorageNode's initialized Future depends up on the completion
   *  of this one.
   */
  def initialize(node: StorageNode): Future[Unit]
}