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
  /** The first boolean is whether to retrieve the object data and the second is whether to retrieve locked transactions */
  type Factory = (ClientSideReadMessenger, ObjectPointer, Boolean, Boolean, UUID) => ReadDriver
}