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

/** 
 *  The content for stored transaction data is split into two separate keys. The "Data" key holds the immutable data consisting of the
 *  transaction description and a copy of the data update content where the "state" key holds the mutable Paxos state and transaction
 *  status/disposition. The primary goal in the separation is to ensure we're not continually re-writing the data updates to disk.
 */
object RocksDBCrashRecoveryLog {
  val TxDataKeyOffset:Byte = 0
  val TxStateKeyOffset:Byte = 1
  
  private def getKey(transactionId: Long, offset:Byte) = {
    val bb = ByteBuffer.allocate(9)
    bb.putLong(0, transactionId)
    bb.put(8, offset)
    bb.array()
  }
  
  private def getDataKey(transactionId:Long) = getKey(transactionId, TxDataKeyOffset)
  private def getStateKey(transactionId:Long) = getKey(transactionId, TxStateKeyOffset)
  
  private def isDataKey(key: Array[Byte]) = key(8) == TxDataKeyOffset
  private def isStateKey(key: Array[Byte]) = key(8) == TxStateKeyOffset
  
  private def getTransactionId(key: Array[Byte]) = ByteBuffer.wrap(key).getLong(0)
  
  private val nulluuid = new UUID(0,0)
  private val nullstore = DataStoreID(nulluuid, 0)
  
  private val nulltxd = new TransactionDescription(nulluuid, 0, 
                                                   ObjectPointer(nulluuid, nulluuid, None, Replication(0,0), new Array[StorePointer](0)),
                                                   0, Nil, Nil, Nil, None)
}

class RocksDBCrashRecoveryLog(dbPath:String)(implicit ec: ExecutionContext) extends CrashRecoveryLog {
  private [this] val db = new BufferedConsistentRocksDB(dbPath)
  private [this] var shuttingDown: Option[Future[Unit]] = None
  private [this] var pendingSaves = Set[Future[Unit]]()
  private [this] var pendingTransactions = Map[UUID,Long]()
  private [this] var savedUpdateContent = Set[Long]()
  private [this] var nextTxId:Long = 0
  
  import RocksDBCrashRecoveryLog._
  
  def initialize(): Future[List[TransactionRecoveryState]] = {
    var states = Map[Long, TransactionRecoveryState]()
    
    db.foreach((key, value) => {
      val txid = getTransactionId(key)
      
      if (txid >= nextTxId)
        nextTxId = txid + 1
      
      states.get(txid) match {
        case None => 
          val trs = if (isDataKey(key)) {
            val td = CRLCodec.transactionDataFromByteArray(value)
            
            pendingTransactions += (td.txd.transactionUUID -> txid)
             
            TransactionRecoveryState(td.dataStoreId, td.txd, td.dataUpdateContent, TransactionDisposition.Undetermined, 
                TransactionStatus.Unresolved, PersistentState(None, None))
          } else {
            val ts = CRLCodec.transactionStateFromByteArray(value)
            
            TransactionRecoveryState(nullstore, nulltxd, None, ts.disposition, ts.status, 
                PersistentState(ts.lastPromisedId, ts.lastAccepted))
          }
          states += (txid -> trs)
          
        case Some(trs) =>
          val newtrs = if (isDataKey(key)) {
            val td = CRLCodec.transactionDataFromByteArray(value)
            
            pendingTransactions += (td.txd.transactionUUID -> txid)
            
            trs.copy(storeId=td.dataStoreId, txd=td.txd, localUpdates=td.dataUpdateContent)
          } else {
            val ts = CRLCodec.transactionStateFromByteArray(value)
            trs.copy(disposition=ts.disposition, status=ts.status, paxosAcceptorState=PersistentState(ts.lastPromisedId, ts.lastAccepted))
          }
          states += (txid -> newtrs)
      }
    }).map(_ => states.values.toList)
  }
  
  def saveTransactionRecoveryState(state: TransactionRecoveryState): Future[Unit] = {
    val (txid, addDataValue, contentSaved) = synchronized { 
      pendingTransactions.get(state.txd.transactionUUID) match {
        case None =>
          val txid = nextTxId
          nextTxId += 1
          pendingTransactions += (state.txd.transactionUUID -> txid)
          (txid, true, false)
        case Some(txid) => (txid, false, savedUpdateContent.contains(txid))
      }
    }
    
    if (addDataValue || (state.localUpdates.isDefined && !contentSaved)) {
      val dataKey = getDataKey(txid) 
      val dataValue = CRLCodec.toTransactionDataByteArray(state.storeId, state.txd, state.localUpdates)
      synchronized {
        if (state.localUpdates.isDefined)
          savedUpdateContent += txid
        db.put(dataKey, dataValue) 
      }
    }
    
    val stateKey = getStateKey(txid)
    val stateValue = CRLCodec.toTransactionStateByteArray(state.disposition, state.status, state.paxosAcceptorState.promised, state.paxosAcceptorState.accepted)
    
    synchronized { 
      val f = db.put(stateKey, stateValue)
      pendingSaves += f
      f onComplete {
        case _ => synchronized { pendingSaves -= f }
      }
      f
    }
  }
  
  def discardTransactionState(txd: TransactionDescription): Unit = confirmedDiscardTransactionState(txd)
  
  def confirmedDiscardTransactionState(txd: TransactionDescription): Future[Unit] = synchronized {
    pendingTransactions.get(txd.transactionUUID) match {
      case None => Future.successful(())
      case Some(txid) =>
        pendingTransactions -= txd.transactionUUID
        savedUpdateContent -= txid
        db.delete(getDataKey(txid))
        db.delete(getStateKey(txid))
    }
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