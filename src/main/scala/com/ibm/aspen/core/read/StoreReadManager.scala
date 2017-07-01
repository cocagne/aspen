package com.ibm.aspen.core.read

import com.ibm.aspen.core.network.StoreSideReadMessenger
import com.ibm.aspen.core.data_store.DataStore
import scala.concurrent.ExecutionContext
import com.ibm.aspen.core.data_store.DataStoreID
import com.ibm.aspen.core.data_store.ObjectError

class StoreReadManager(
    val storeMessenger: StoreSideReadMessenger,
    initialStores: List[DataStore])(implicit ec: ExecutionContext) {
  
  private[this] var dataStores: Map[DataStoreID,DataStore] = initialStores.map(ds => (ds.storeId -> ds)).toMap
  
  def addStore(ds: DataStore) = synchronized { dataStores += (ds.storeId -> ds) }
  
  def removeStore(ds: DataStore) = synchronized { dataStores -= ds.storeId }
  
  def receiveReadMessage(m: Read): Unit = {
    synchronized { dataStores.get(m.toStore) } foreach {
      ds => 
        val f = ds.getObject(m.objectPointer)
                                     
        f onSuccess {
          case result => 
            val response = result match {
              case Left(err) => err match {
                case ObjectError.InvalidLocalPointer => Left(ReadError.InvalidLocalPointer)
                case ObjectError.ObjectMismatch => Left(ReadError.ObjectMismatch)
                case ObjectError.CorruptedObject => Left(ReadError.CorruptedObject)
                //
                // The following two should not be possible for a simple read since we're not checking versions/counts
                // This probably means we should break out object read errors from object check errors
                //
                case ObjectError.RefcountMismatch => Left(ReadError.UnexpectedInternalError)
                case ObjectError.RevisionMismatch => Left(ReadError.UnexpectedInternalError)
              }
              
              case Right((cs, data)) => Right(ReadResponse.CurrentState(cs.revision, cs.refcount, if (m.returnObjectData) Some(data) else None,
                                                                        if (m.returnLockedTransaction) cs.lockedTransaction else None))
            }
            storeMessenger.send(m.fromClient, ReadResponse(m.toStore, m.readUUID, response))
        }
    }
  }
}