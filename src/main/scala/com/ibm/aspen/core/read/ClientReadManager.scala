package com.ibm.aspen.core.read

import com.ibm.aspen.core.network.ClientSideReadMessenger
import scala.concurrent.ExecutionContext
import java.util.UUID
import scala.concurrent.Future
import com.ibm.aspen.core.data_store.DataStoreID
import com.ibm.aspen.core.objects.ObjectPointer
import com.ibm.aspen.core.network.ClientSideReadMessageReceiver
import com.ibm.aspen.core.objects.ObjectState
import com.ibm.aspen.core.transaction.TransactionDescription
import com.ibm.aspen.core.data_store.Lock

class ClientReadManager(val clientMessenger: ClientSideReadMessenger)(implicit ec: ExecutionContext) extends ClientSideReadMessageReceiver {
  
  private[this] var outstandingReads = Map[UUID, ReadDriver]()
  
  def receive(m: ReadResponse): Unit = { 
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
      readType: ReadType,
      retrieveTransactionLocks:Boolean=false, 
      driverFactory: ReadDriver.Factory): Future[Either[ReadError, (ObjectState, Option[Map[DataStoreID, List[Lock]]])]] = {
    
    val readUUID = UUID.randomUUID()
    
    val driver = driverFactory(clientMessenger, objectPointer, readType, retrieveTransactionLocks, readUUID)
                                      
    synchronized { outstandingReads += (readUUID -> driver) }
    
    driver.begin()
    
    driver.readResult onComplete {
      case _ => synchronized { outstandingReads -= readUUID }
    }
    
    driver.readResult
  }
}