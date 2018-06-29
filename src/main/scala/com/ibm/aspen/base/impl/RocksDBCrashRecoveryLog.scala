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
 */
object RocksDBCrashRecoveryLog {
  val TxDataKeyOffset:Byte = 0
  val TxStateKeyOffset:Byte = 1
  val AllocKeyOffset: Byte = 2
  
  // Key = XOR of poolUUID & transactionUUID with the first byte set to the poolIndex and the last to the key offset
  private def getKey(storeId: DataStoreID, transactionUUID: UUID, oobjectUUID: Option[UUID], offset:Byte): Array[Byte] = {
    val bb = ByteBuffer.allocate(16)
    val omsb = oobjectUUID.map(uuid => uuid.getMostSignificantBits).getOrElse(0L)
    val olsb = oobjectUUID.map(uuid => uuid.getLeastSignificantBits).getOrElse(0L)
    bb.putLong(0, transactionUUID.getMostSignificantBits ^ storeId.poolUUID.getMostSignificantBits ^ omsb)
    bb.putLong(8, transactionUUID.getLeastSignificantBits ^ storeId.poolUUID.getLeastSignificantBits ^ olsb)
    bb.put(0, storeId.poolIndex)
    bb.put(15, offset)
    bb.array()
  }
  
  private def getDataKey(storeId: DataStoreID, transactionUUID: UUID) = getKey(storeId, transactionUUID, None, TxDataKeyOffset)
  private def getStateKey(storeId: DataStoreID, transactionUUID: UUID) = getKey(storeId, transactionUUID, None, TxStateKeyOffset)
  private def getAllocKey(storeId: DataStoreID, transactionUUID: UUID, objectUUID: UUID) = getKey(storeId, transactionUUID, Some(objectUUID), AllocKeyOffset)
  
  private def isDataKey(key: Array[Byte]) = key(15) == TxDataKeyOffset
  private def isStateKey(key: Array[Byte]) = key(15) == TxStateKeyOffset
  private def isAllocKey(key: Array[Byte]) = key(15) == AllocKeyOffset
}

class RocksDBCrashRecoveryLog(dbPath:String)(implicit ec: ExecutionContext) extends CrashRecoveryLog {
  private [this] val db = new BufferedConsistentRocksDB(dbPath)
  private [this] var shuttingDown: Option[Future[Unit]] = None
  private [this] var pendingSaves = Set[Future[Unit]]()
  private [this] var pendingTransactions = Map[UUID,TransactionRecoveryState]()
  private [this] var pendingAllocations = Map[UUID,List[AllocationRecoveryState]]()
  
  import RocksDBCrashRecoveryLog._
  
  def initialize(): Future[(List[TransactionRecoveryState], List[AllocationRecoveryState])] = synchronized {
    
    var txData = Map[UUID, CRLCodec.TransactionData]()
    var txState = Map[UUID, CRLCodec.TransactionState]()
    
    // Creates a UUID from the key with the offset zeored (so state & data keys resolve to the same UUID)
    def keyToUUID(key: Array[Byte]): UUID = {
      val bb = ByteBuffer.allocate(16)
      bb.put(key)
      bb.put(15, 0)
      bb.position(0)
      val msb = bb.getLong()
      val lsb = bb.getLong()
      return new UUID(msb, lsb)
    }
    
    db.foreach((key, value) => {
      
      if (isAllocKey(key)) {
        val ars = CRLCodec.allocationFromByteArray(value)
        val lst = ars :: pendingAllocations.getOrElse(ars.allocationTransactionUUID, Nil)
        pendingAllocations += (ars.allocationTransactionUUID -> lst)
      }
      else if(isDataKey(key)) {
        txData = txData + (keyToUUID(key) -> CRLCodec.transactionDataFromByteArray(value))
      }
      else {
        txState = txState + (keyToUUID(key) -> CRLCodec.transactionStateFromByteArray(value))
      }
    }) map { _ =>
      
      txState foreach { t =>
        val (key, ts) = t
        txData.get(key) foreach { td =>
          val trs = TransactionRecoveryState(td.dataStoreId, td.txd, td.dataUpdateContent, ts.disposition, ts.status, 
                                             PersistentState(ts.lastPromisedId, ts.lastAccepted))
          pendingTransactions += (td.txd.transactionUUID -> trs)
        }
      }
      
      (pendingTransactions.values.toList, pendingAllocations.values.toList.flatten)
    }
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
    pendingAllocations.valuesIterator.foreach { lars => lars.foreach { ars => 
      m.get(ars.storeId) match {
        case None => m += (ars.storeId -> List(ars))
        case Some(l) =>
          val newList = ars:: l
          m += (ars.storeId -> newList)
      }
    }}
    
    m
  }
  
  def getAllocationRecoveryStateForStore(storeId: DataStoreID): List[AllocationRecoveryState] = {
    // Get a snapshot of the mutable dictionary and iterate over that to minimize the lock duration
    val pa = synchronized { pendingAllocations }
    pa.values.flatten.filter(ars => ars.storeId == storeId).toList
  }
  
  def saveTransactionRecoveryState(state: TransactionRecoveryState): Future[Unit] = {
    val prevTrs = synchronized { pendingTransactions.get(state.txd.transactionUUID) }
    
    class SaveData {
      val dataKey = getDataKey(state.storeId, state.txd.transactionUUID) 
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
    
    val stateKey = getStateKey(state.storeId, state.txd.transactionUUID)
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
  
  def discardTransactionState(storeId: DataStoreID, txd: TransactionDescription): Unit = confirmedDiscardTransactionState(storeId, txd)
  
  def confirmedDiscardTransactionState(storeId: DataStoreID, txd: TransactionDescription): Future[Unit] = synchronized {
    pendingTransactions.get(txd.transactionUUID) match {
      case None => Future.successful(())
      case Some(txid) =>
        pendingTransactions -= txd.transactionUUID
        db.delete(getDataKey(storeId, txd.transactionUUID))
        db.delete(getStateKey(storeId, txd.transactionUUID))
    }
  }
  
  def saveAllocationRecoveryState(state: AllocationRecoveryState): Future[Unit] = {
    val value = CRLCodec.toAllocationByteArray(state)
    synchronized {
      db.put(getAllocKey(state.storeId, state.allocationTransactionUUID, state.newObjectUUID), value)
    }
  }
  
  def discardAllocationState(state: AllocationRecoveryState): Unit = synchronized { 
    db.delete(getAllocKey(state.storeId, state.allocationTransactionUUID, state.newObjectUUID))
  }
  
  def close(): Future[Unit] = synchronized {
    shuttingDown match {
      case None =>
        val f = Future.sequence(pendingSaves).flatMap(_=> db.close())
        
        shuttingDown = Some(f)
        
        f
        
      case Some(f) => f
    }
  }
}