package com.ibm.aspen.base.impl

import scala.concurrent.ExecutionContext
import com.ibm.aspen.core.crl.CrashRecoveryLog
import scala.concurrent.Future
import com.ibm.aspen.core.transaction.TransactionRecoveryState
import java.nio.ByteBuffer
import com.ibm.aspen.core.transaction.TransactionDescription
import java.util.UUID
import com.google.flatbuffers.FlatBufferBuilder
import com.ibm.aspen.core.transaction.TransactionDisposition
import com.ibm.aspen.core.transaction.TransactionStatus
import com.ibm.aspen.core.transaction.paxos.PersistentState
import com.ibm.aspen.core.objects.ObjectPointer
import com.ibm.aspen.core.ida.Replication
import com.ibm.aspen.core.objects.StorePointer
import com.ibm.aspen.core.data_store.DataStoreID
import com.ibm.aspen.core.allocation.AllocationRecoveryState

/** 
 *  General idea is that we'll separate transaction Data and State into two separate keys. This allows the state to be continually
 *  overwritten with the most recent value while allowing the Data to be written only once. 
 *  
 *  Key = 17-byte Array
 *     DataKey  = 16-byte Transaction UUID + 0
 *     StateKey = 16-byte Transaction UUID + 1
 *  
 */
object RocksDBCrashRecoveryLog {
  val TxDataKeyOffset:Byte = 0
  val TxStateKeyOffset:Byte = 1
  val AllocKeyOffset: Byte = 2
  
  private def getKey(transactionUUID: UUID, offset:Byte): Array[Byte] = {
    val bb = ByteBuffer.allocate(17)
    bb.putLong(0, transactionUUID.getMostSignificantBits)
    bb.putLong(8, transactionUUID.getLeastSignificantBits)
    bb.put(16, offset)
    bb.array()
  }
  
  private def getDataKey(transactionUUID: UUID) = getKey(transactionUUID, TxDataKeyOffset)
  private def getStateKey(transactionUUID: UUID) = getKey(transactionUUID, TxStateKeyOffset)
  private def getAllocKey(transactionUUID: UUID) = getKey(transactionUUID, AllocKeyOffset)
  
  private def isDataKey(key: Array[Byte]) = key(16) == TxDataKeyOffset
  private def isStateKey(key: Array[Byte]) = key(16) == TxStateKeyOffset
  private def isAllocKey(key: Array[Byte]) = key(16) == AllocKeyOffset
  
  private def getTransactionUUID(key: Array[Byte]): UUID = {
    val bb = ByteBuffer.wrap(key)
    val msb = bb.getLong()
    val lsb = bb.getLong()
    return new UUID(msb, lsb)
  }
  
  private val NullUUID = new UUID(0,0)
  private val NullStore = DataStoreID(NullUUID, 0)
  
  private val NullTXD = new TransactionDescription(NullUUID, 0, 
                                                   ObjectPointer(NullUUID, NullUUID, None, Replication(0,0), new Array[StorePointer](0)),
                                                   0, Nil, Nil, None)
}

class RocksDBCrashRecoveryLog(dbPath:String)(implicit ec: ExecutionContext) extends CrashRecoveryLog {
  private [this] val db = new BufferedConsistentRocksDB(dbPath)
  private [this] var shuttingDown: Option[Future[Unit]] = None
  private [this] var pendingSaves = Set[Future[Unit]]()
  private [this] var pendingTransactions = Map[UUID,TransactionRecoveryState]()
  private [this] var pendingAllocations = Map[UUID,AllocationRecoveryState]()
  
  import RocksDBCrashRecoveryLog._
  
  def initialize(): Future[(List[TransactionRecoveryState], List[AllocationRecoveryState])] = synchronized {
    
    db.foreach((key, value) => {
      val txUUID = getTransactionUUID(key)
      
      if (isAllocKey(key)) {
        pendingAllocations += (txUUID -> CRLCodec.allocationFromByteArray(value))
      } else {
      
        val trs = pendingTransactions.get(txUUID) match {
          case None =>
            // Fill in what we can for initial TransactionRecvoveryState. Later state/data updates will overwrite the placeholder values
            if (isDataKey(key)) {
              val td = CRLCodec.transactionDataFromByteArray(value)
               
              TransactionRecoveryState(td.dataStoreId, td.txd, td.dataUpdateContent, TransactionDisposition.Undetermined, 
                  TransactionStatus.Unresolved, PersistentState(None, None))
            } else {
              val ts = CRLCodec.transactionStateFromByteArray(value)
              
              TransactionRecoveryState(NullStore, NullTXD, None, ts.disposition, ts.status, 
                  PersistentState(ts.lastPromisedId, ts.lastAccepted))
            }
            
          case Some(trs) =>
            // Overwrite the state/data portions of the current value for the TransactionRecoveryState
            if (isDataKey(key)) {
              val td = CRLCodec.transactionDataFromByteArray(value)
              
              trs.copy(storeId=td.dataStoreId, txd=td.txd, localUpdates=td.dataUpdateContent)
            } else {
              val ts = CRLCodec.transactionStateFromByteArray(value)
              trs.copy(disposition=ts.disposition, status=ts.status, paxosAcceptorState=PersistentState(ts.lastPromisedId, ts.lastAccepted))
            }
        }
        
        pendingTransactions += (txUUID -> trs)
      }
    }).map(_ => (pendingTransactions.values.toList, pendingAllocations.values.toList))
  }
  
  def getFullTransactionRecoveryState(): Map[DataStoreID, List[TransactionRecoveryState]] = synchronized {
    var m = Map[DataStoreID, List[TransactionRecoveryState]]()
    pendingTransactions.valuesIterator.foreach(trs => m.get(trs.storeId) match {
      case None => m += (trs.storeId -> List(trs))
      case Some(l) =>
        val newList = trs:: l
        m += (trs.storeId -> newList)
    })
    m
  }
  
  def getTransactionRecoveryStateForStore(storeId: DataStoreID): List[TransactionRecoveryState] = {
    // Get a snapshot of the mutable dictionary and iterate over that to minimize the lock duration
    val pt = synchronized { pendingTransactions }
    pt.foldLeft(List[TransactionRecoveryState]())( (l, t) => if (t._2.storeId == storeId) t._2 :: l else l )
  }
  
  def getFullAllocationRecoveryState(): Map[DataStoreID, List[AllocationRecoveryState]] = synchronized {
    var m = Map[DataStoreID, List[AllocationRecoveryState]]()
    pendingAllocations.valuesIterator.foreach(ars => m.get(ars.storeId) match {
      case None => m += (ars.storeId -> List(ars))
      case Some(l) =>
        val newList = ars:: l
        m += (ars.storeId -> newList)
    })
    m
  }
  
  def getAllocationRecoveryStateForStore(storeId: DataStoreID): List[AllocationRecoveryState] = {
    // Get a snapshot of the mutable dictionary and iterate over that to minimize the lock duration
    val pa = synchronized { pendingAllocations }
    pa.foldLeft(List[AllocationRecoveryState]())( (l, t) => if (t._2.storeId == storeId) t._2 :: l else l )
  }
  
  def saveTransactionRecoveryState(state: TransactionRecoveryState): Future[Unit] = {
    val prevTrs = synchronized { pendingTransactions.get(state.txd.transactionUUID) }
    
    class SaveData {
      val dataKey = getDataKey(state.txd.transactionUUID) 
      val dataValue = CRLCodec.toTransactionDataByteArray(state.storeId, state.txd, state.localUpdates)
    }
    
    val (newTrs, saveData) = prevTrs match {
      case None => 
        // First time we've seen this transaction. Ensure data (which includes txd) is saved to disk. If the localUpdates arrive at a later
        // time, we'll overwrite the data key with no harm done
        val trs = TransactionRecoveryState(state.storeId, state.txd, state.localUpdates, state.disposition, state.status, state.paxosAcceptorState)
        val sdata = Some(new SaveData)
        (trs, sdata)
        
      case Some(ptrs) => if (!ptrs.localUpdates.isDefined && state.localUpdates.isDefined) {
          // We received the local update data late. Ensure it's written to disk and tracked by our in-mem recovery state object
          val trs = ptrs.copy(localUpdates=state.localUpdates, disposition=state.disposition, status=state.status, paxosAcceptorState=state.paxosAcceptorState)
          (trs, Some(new SaveData))
        } else {
          // Only need to copy and save updated transaction state. Data is already written or we don't have it
          val trs = ptrs.copy(disposition=state.disposition, status=state.status, paxosAcceptorState=state.paxosAcceptorState)
          (trs, None)
        }
    }
    
    val stateKey = getStateKey(state.txd.transactionUUID)
    val stateValue = CRLCodec.toTransactionStateByteArray(state.disposition, state.status, state.paxosAcceptorState.promised, state.paxosAcceptorState.accepted)
    
    synchronized {
      
      saveData.foreach(s => db.put(s.dataKey, s.dataValue))
      val f = db.put(stateKey, stateValue)
      
      pendingSaves += f
      f onComplete {
        case _ => synchronized { pendingSaves -= f }
      }
      
      pendingTransactions += (state.txd.transactionUUID -> newTrs)
      f
    }
  }
  
  def discardTransactionState(storeId: DataStoreID, txd: TransactionDescription): Unit = confirmedDiscardTransactionState(txd)
  
  def confirmedDiscardTransactionState(txd: TransactionDescription): Future[Unit] = synchronized {
    pendingTransactions.get(txd.transactionUUID) match {
      case None => Future.successful(())
      case Some(txid) =>
        pendingTransactions -= txd.transactionUUID
        db.delete(getDataKey(txd.transactionUUID))
        db.delete(getStateKey(txd.transactionUUID))
    }
  }
  
  def saveAllocationRecoveryState(state: AllocationRecoveryState): Future[Unit] = {
    val value = CRLCodec.toAllocationByteArray(state)
    synchronized {
      db.put(getAllocKey(state.allocationTransactionUUID), value)
    }
  }
  
  def discardAllocationState(storeId: DataStoreID, allocationTransactionUUID: UUID): Unit = synchronized { 
    db.delete(getAllocKey(allocationTransactionUUID))
  }
  
  def close(): Future[Unit] = synchronized {
    shuttingDown match {
      case None =>
        val f = Future.sequence(pendingSaves) 
        
        f onComplete { 
          case _ => db.close()
        }
        
        val fu = f.map(_ => ())
        
        shuttingDown = Some(fu)
        
        fu
        
      case Some(f) => f
    }
  }
  
  // For unit tests only
  private [impl] def immediateClose() = db.close()
}