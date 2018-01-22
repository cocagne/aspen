package com.ibm.aspen.core.read

import scala.concurrent.Future
import com.ibm.aspen.core.data_store.DataStoreID
import com.ibm.aspen.core.network.ClientSideReadMessenger
import java.util.UUID
import com.ibm.aspen.core.objects.ObjectPointer
import com.ibm.aspen.core.objects.ObjectState
import com.ibm.aspen.core.transaction.TransactionDescription
import com.ibm.aspen.core.data_store.Lock

trait ReadDriver {
  def readResult: Future[Either[ReadError, (ObjectState, Option[Map[DataStoreID, List[Lock]]])]]
  
  /** Called to begin the read process. Read messages must not be sent until this method is called */
  def begin(): Unit
  
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