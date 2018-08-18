package com.ibm.aspen.core.data_store

import java.util.UUID
import com.ibm.aspen.core.objects.keyvalue.Key
import com.ibm.aspen.core.objects.keyvalue.Value
import com.ibm.aspen.core.objects.keyvalue.KeyValueObjectStoreState
import com.ibm.aspen.core.DataBuffer
import com.ibm.aspen.core.objects.keyvalue.SetMin
import com.ibm.aspen.core.objects.keyvalue.SetMax
import com.ibm.aspen.core.objects.keyvalue.SetLeft
import com.ibm.aspen.core.objects.keyvalue.SetRight
import com.ibm.aspen.core.objects.keyvalue.Insert
import com.ibm.aspen.core.objects.keyvalue.Delete
import com.ibm.aspen.core.transaction.TransactionDescription
import scala.concurrent.Future
import scala.concurrent.Promise
import com.ibm.aspen.core.objects.ObjectRevision
import com.ibm.aspen.core.HLCTimestamp


class MutableKeyValueObject(
    objectId: StoreObjectID, 
    initialOperation: UUID, 
    loader: MutableObjectLoader,
    ostate: Option[(ObjectMetadata, DataBuffer)]) extends MutableObject(objectId, initialOperation, loader, ostate) {
  
  protected var okvoss: Option[KeyValueObjectStoreState] = None
  
  var keyRevisionReadLocks: Map[Key, Map[UUID,TransactionDescription]] = Map()
  var keyRevisionWriteLocks: Map[Key, TransactionDescription] = Map()

  override def getTransactionPreventingRevisionWriteLock(ignoreTxd: TransactionDescription): Option[TransactionDescription] = {
    super.getTransactionPreventingRevisionWriteLock(ignoreTxd) match {
      case Some(txd) => Some(txd)
      case None => if (keyRevisionWriteLocks.isEmpty) {
        if (keyRevisionReadLocks.isEmpty)
          None
        else
          Some(keyRevisionReadLocks.head._2.head._2)
      } else
        Some(keyRevisionWriteLocks.head._2)
    }
  }
  
  override def getTransactionPreventingRevisionReadLock(ignoreTxd: TransactionDescription): Option[TransactionDescription] = {
    super.getTransactionPreventingRevisionReadLock(ignoreTxd) match {
      case Some(txd) => Some(txd)
      case None => if (keyRevisionWriteLocks.isEmpty) None else Some(keyRevisionWriteLocks.head._2)
    }
  }
    
  /** This MUST be called before using any of the variables defined in this class */
  def storeState: KeyValueObjectStoreState = {
    assert(dataLoaded)
    okvoss match {
      case Some(kvoss) => kvoss
      case None => 
        val kvoss = KeyValueObjectStoreState.decode(dataBuffer)
        okvoss = Some(kvoss)
        kvoss
    }
  }
  
  def restore(meta: ObjectMetadata, kvoss: KeyValueObjectStoreState): Unit = {
    val db = KeyValueObjectStoreState.encode(kvoss)
    setRebuildState(meta, db)
    okvoss = Some(kvoss)
  }
  
  def restore(meta: ObjectMetadata, data: DataBuffer): Unit = {
    val kvoss = KeyValueObjectStoreState.decode(data)
    setRebuildState(meta, data)
    okvoss = Some(kvoss)
  }
  
  def update(db: DataBuffer, txRevision: ObjectRevision, txTimestamp: HLCTimestamp): Unit = {
    val kvoss = storeState
    
    val (partialKvoss, deletes) = KeyValueObjectStoreState.decodeOps(db, txRevision, txTimestamp)
    
    okvoss = Some(kvoss.update(partialKvoss, deletes))

    dataBuffer = KeyValueObjectStoreState.encode(kvoss)
  }
}