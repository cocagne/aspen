package com.ibm.aspen.core.read

import com.ibm.aspen.core.network.ClientSideReadMessenger
import scala.concurrent.ExecutionContext
import java.util.UUID
import scala.concurrent.Future
import com.ibm.aspen.core.data_store.DataStoreID
import com.ibm.aspen.core.data_store.CurrentObjectState
import com.ibm.aspen.core.objects.ObjectPointer

class ClientReadManager(val clientMessenger: ClientSideReadMessenger)(implicit ec: ExecutionContext) {
  
  private[this] var outstandingReads = Map[UUID, ReadDriver]()
  
  def receiveReadResponseMessage(m: ReadResponse): Unit = { 
    synchronized { outstandingReads.get(m.readUUID) } foreach {
      driver => driver.receiveReadResponse(m)
    }  
  }
  
  /** Creates a ReadDriver from the passed-in factory function and returns a Future to the eventual result.
   * 
   * The purpose of using a factory here is to allow for flexibility in the strategies used to drive the reads. Reads of objects
   * that are known to exist at the same site as the client might use smaller timeouts or more more aggressive retry logic.
   * 
   * driverFactory - First boolean is for retrieve data, second is for retrieve transaction locks
   */
  def read(
      objectPointer: ObjectPointer, 
      retrieveData:Boolean=true, 
      retrieveTransactionLocks:Boolean=false, 
      driverFactory: ReadDriver.Factory): Future[Either[ReadError, ObjectState]] = {
    
    val readUUID = UUID.randomUUID()
    
    val driver = driverFactory(clientMessenger, objectPointer, retrieveData, retrieveTransactionLocks, readUUID)
                                      
    synchronized { outstandingReads += (readUUID -> driver) }
    
    driver.readResult onComplete {
      case _ => synchronized { outstandingReads -= readUUID }
    }
    
    driver.readResult
  }
}