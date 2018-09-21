package com.ibm.aspen.core.data_store

import java.util.UUID

import com.ibm.aspen.core.objects.ObjectRevision
import com.ibm.aspen.core.objects.keyvalue.Key
import com.ibm.aspen.core.transaction.TransactionDescription
import com.ibm.aspen.core.{DataBuffer, HLCTimestamp}


class MutableKeyValueObject(objectId: StoreObjectID,
                            initialOperation: UUID,
                            loader: MutableObjectLoader,
                            allocationState: Option[(ObjectMetadata, DataBuffer, ObjectRevision, HLCTimestamp)]) extends
  MutableObject(objectId, initialOperation, loader, allocationState.isDefined) {

  private[this] var encodedData: DataBuffer = DataBuffer.Empty

  // if we have initial data, overwrite the data buffer with the converted content
  protected var okvoss: Option[StoreKeyValueObjectContent] = allocationState.map { t =>
    val (meta, opsData, revision, ts) = t

    // State-modification operations are sent over the network. On disk is just the current state. For allocation we'll create
    // and empty object then apply the state-modification operations provided in the allocation message
    val newKvoss = StoreKeyValueObjectContent().update(opsData, revision, ts)

    this.meta = meta
    encodedData = newKvoss.encode()
    
    newKvoss
  }

  def data: DataBuffer = synchronized { encodedData }

  /** Called when state is loaded from the store */
  protected def stateLoadedFromBackingStore(m: ObjectMetadata, odb: Option[DataBuffer]): Unit = synchronized {
    this.meta = m
    odb.foreach(db => restore(m, db))
  }
  
  var keyRevisionReadLocks: Map[Key, Map[UUID,TransactionDescription]] = Map()
  var keyRevisionWriteLocks: Map[Key, TransactionDescription] = Map()
  
  override def writeLocks: Set[UUID] = {
    keyRevisionWriteLocks.foldLeft(super.writeLocks)((s, t) => s + t._2.transactionUUID)
  }

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
  def storeState: StoreKeyValueObjectContent = synchronized {
    assert(bothLoaded)
    okvoss match {
      case Some(kvoss) => kvoss
      case None => 
        val kvoss = StoreKeyValueObjectContent(encodedData)
        okvoss = Some(kvoss)
        kvoss
    }
  }
  
  def restore(meta: ObjectMetadata, kvoss: StoreKeyValueObjectContent): Unit = synchronized {
    this.meta = meta
    encodedData = kvoss.encode()
    okvoss = Some(kvoss)
  }
  
  def restore(meta: ObjectMetadata, data: DataBuffer): Unit = synchronized {
    val kvoss = StoreKeyValueObjectContent(data)
    restore(meta, kvoss)
  }
  
  def update(db: DataBuffer, txRevision: ObjectRevision, txTimestamp: HLCTimestamp): Unit = synchronized {
    
    val newKvoss = storeState.update(db, txRevision, txTimestamp)
    
    okvoss = Some(newKvoss)
    encodedData = newKvoss.encode()
  }
}