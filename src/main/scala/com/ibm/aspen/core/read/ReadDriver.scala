package com.ibm.aspen.core.read

import scala.concurrent.Future
import com.ibm.aspen.core.data_store.DataStoreID
import com.ibm.aspen.core.data_store.CurrentObjectState
import com.ibm.aspen.core.network.ClientSideReadMessenger
import java.util.UUID

trait ReadDriver {
  def readResult: Future[Either[ReadError, ObjectState]]
  
  def receiveReadResponse(response:ReadResponse): Unit
}
