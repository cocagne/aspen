package com.ibm.aspen.core.read

import scala.concurrent.Future
import com.ibm.aspen.core.data_store.DataStoreID
import com.ibm.aspen.core.data_store.CurrentObjectState
import com.ibm.aspen.core.network.ClientSideReadMessenger
import java.util.UUID
import com.ibm.aspen.core.objects.ObjectPointer

trait ReadDriver {
  def readResult: Future[Either[ReadError, ObjectState]]
  
  def receiveReadResponse(response:ReadResponse): Unit
}

object ReadDriver {
  /**
   * Signature: 
   * 
   * Factory( clientMessenger: ClientSideReadMessenger
   *          objectPointer: ObjectPointer,
   *          retrieveObjectData: Boolean,
   *          retrieveLockedTransaction: Boolean,
   * ,        readUUID:UUID)
   */
  type Factory = (ClientSideReadMessenger, ObjectPointer, Boolean, Boolean, UUID) => ReadDriver
}