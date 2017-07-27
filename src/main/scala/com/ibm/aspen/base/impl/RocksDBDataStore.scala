package com.ibm.aspen.base.impl

import com.ibm.aspen.core.data_store.DataStore
import java.util.UUID
import com.ibm.aspen.core.transaction.TransactionDescription
import scala.concurrent.Future
import com.ibm.aspen.core.objects.ObjectPointer
import com.ibm.aspen.core.objects.ObjectRevision
import com.ibm.aspen.core.objects.StorePointer
import com.ibm.aspen.core.objects.ObjectRefcount
import com.ibm.aspen.core.allocation.AllocationError
import java.nio.ByteBuffer
import com.ibm.aspen.core.data_store.DataStoreID
import com.ibm.aspen.core.data_store.ObjectError
import com.ibm.aspen.core.data_store.CurrentObjectState
import scala.concurrent.ExecutionContext

object RocksDBDataStore {
  val StateIndex:Byte = 0
  val DataIndex:Byte = 1
  
  private [this] def tokey(objectUUID:UUID, index:Byte) = {
    val bb = ByteBuffer.allocate(17)
    bb.putLong(0, objectUUID.getMostSignificantBits)
    bb.putLong(8, objectUUID.getLeastSignificantBits)
    bb.put(16, index)
    bb.array()
  }
  
  private def stateKey(objectUUID:UUID) = tokey(objectUUID, StateIndex)
  private def dataKey(objectUUID:UUID) = tokey(objectUUID, DataIndex)
  
  private def stateToBytes(rev: ObjectRevision, ref: ObjectRefcount, lastTransactionUUID: UUID): Array[Byte] = {
    val bb = ByteBuffer.allocate(32)
    bb.putInt(0, rev.overwriteCount)
    bb.putInt(4, rev.currentSize)
    bb.putInt(8, ref.updateSerial)
    bb.putInt(12, ref.count)
    bb.putLong(16, lastTransactionUUID.getMostSignificantBits)
    bb.putLong(24, lastTransactionUUID.getLeastSignificantBits)
    bb.array()
  }
  private def bytesToState(buf:Array[Byte]): (ObjectRevision, ObjectRefcount, UUID) = {
    val bb = ByteBuffer.wrap(buf)
    val rev = ObjectRevision(bb.getInt(0), bb.getInt(4))
    val ref = ObjectRefcount(bb.getInt(8), bb.getInt(12)) 
    val txuuid = new UUID(bb.getLong(16), bb.getLong(24))
    (rev, ref, txuuid)
  }
  
  private class WorkingState(
      var revision:ObjectRevision, 
      var refcount: ObjectRefcount, 
      var lastTxUUID: UUID,
      var data: ByteBuffer, 
      var lockedTransaction: Option[TransactionDescription],
      var pendingTransactions: Set[TransactionDescription])
}

abstract class RocksDBDataStore(
    val storeId: DataStoreID,
    initialLockedTransactions: List[TransactionDescription],
    dbPath:String)(implicit ec: ExecutionContext) extends DataStore {
  
  import RocksDBDataStore._
  
  private [this] val db = new BufferedConsistentRocksDB(dbPath)
  
  /** Holds the current object state while transactions are outstanding and/or data has yet to be committed to disk */
  private [this] var workingState = Map[UUID, WorkingState]()
  
  /** TODO Failure handling. Currently we never check to see if the allocation succeeded/failed
   */
  def allocateNewObject(objectUUID: UUID, 
                        size: Option[Int], 
                        initialContent: ByteBuffer,
                        initialRefcount: ObjectRefcount,
                        allocationTransactionUUID: UUID,
                        allocatingObject: ObjectPointer,
                        allocatingObjectRevision: ObjectRevision): Future[Either[AllocationError.Value, StorePointer]] = {
    val f = synchronized {
      db.put(stateKey(objectUUID), stateToBytes(ObjectRevision(0, initialContent.capacity), initialRefcount, allocationTransactionUUID))
      val buf = if (!initialContent.isDirect()) 
        initialContent.array()
        else {
          val a = new Array[Byte](initialContent.capacity)
          initialContent.get(a)
          a
        }
      db.put(dataKey(objectUUID), buf)
    }
    
    f.map(_ => Right(StorePointer(storeId.poolIndex, new Array[Byte](0))))
  }
  
  def getObject(objectPointer: ObjectPointer, storePointer: StorePointer): Future[Either[ObjectError.Value, (CurrentObjectState,ByteBuffer)]] = {
    synchronized {workingState.get(objectPointer.uuid)} match {
      case Some(ws) => Future.successful(Right((CurrentObjectState(objectPointer.uuid, ws.revision, ws.refcount, ws.lastTxUUID, ws.lockedTransaction), ws.data)))
      case None =>
        val fstate = db.get(stateKey(objectPointer.uuid))
        val fdata = db.get(dataKey(objectPointer.uuid))
        for {
          ostate <- fstate
          odata <- fdata
        } yield {
          (ostate, odata) match {
            case (Some(stateBuf), Some(dataBuf)) =>
              val (revision, refcount, lastTxUUID) = bytesToState(stateBuf)
              Right((CurrentObjectState(objectPointer.uuid, revision, refcount, lastTxUUID, None), ByteBuffer.wrap(dataBuf)))
            case _ => Left(ObjectError.InvalidLocalPointer)
          }
        }
    }
  }
  
  
  def getCurrentObjectState(txd: TransactionDescription): Future[Map[UUID, Either[ObjectError.Value, CurrentObjectState]]] = {
    val localObjects = txd.allReferencedObjectsSet.filter(op => op.poolUUID == storeId.poolUUID && op.storePointers.find(_.poolIndex == storeId.poolIndex).isDefined)
    
    val futureSet = localObjects.map(op => getObject(op).map(r => r match {
      case Left(err) => (op.uuid -> Left(err))
      case Right((state, data)) => (op.uuid -> Right(state))
    }))
    
    Future.sequence(futureSet).map( _.toMap )
  }
  
  
  /** Locks all objects referenced by the transaction or returns a map of collisions and/or errors. Note that this method
   *  must detect Revision and Refcount mismatch errors. getCurrentObjectState is used by transactions to do initial error
   *  checking on the refcount and revision but it is possible for those values to change between that call and this call.
   */
//  def lockOrCollide(txd: TransactionDescription): Option[Map[UUID, Either[ObjectError.Value, TransactionDescription]]] = synchronized {
//    
//  }
  
  
  /** Commits the transaction changes and returns a Future to the completion of the commit operation.
   *  
   *  This method always returns Success() since there are no recovery steps the transaction logic can take for failures
   *  that occur after the commit decision has been made. 
   */
  def commitTransactionUpdates(txd: TransactionDescription, localUpdates: Option[Array[ByteBuffer]]): Future[Unit]
  
  
  /** Called at the end of each transaction to ensure all object locks are released.
   *  
   *  For successful transactions, commitTransactionUpdates will be called first and it should release the
   *  locks while the finalization actions run. Both committed and aborted transactions call this method.
   * 
   */
  def discardTransaction(txd: TransactionDescription): Unit
}