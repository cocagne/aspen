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


class MutableKeyValueObject(
    objectId: StoreObjectID, 
    initialOperation: UUID, 
    loader: MutableObjectLoader) extends MutableObject(objectId, initialOperation, loader) {
  
  protected var contentValid = false
  
  var minimum: Option[Array[Byte]] = None
  var maximum: Option[Array[Byte]] = None
  var idaEncodedLeft: Option[Array[Byte]] = None
  var idaEncodedRight: Option[Array[Byte]] = None
  var idaEncodedContents: Map[Key, Value] = Map()
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

  /** This MUST be called before using any of the variables defined in this class */
  def parseKeyValueContent(): Unit = {
    assert(dataLoaded)
    if (!contentValid) {
      val kvos = KeyValueObjectStoreState(0, data)
      minimum = kvos.minimum
      maximum = kvos.maximum
      idaEncodedLeft = kvos.idaEncodedLeft
      idaEncodedRight = kvos.idaEncodedRight
      idaEncodedContents = kvos.idaEncodedContents
      contentValid = true
    }
  }
  
  /** Called when the data for a KeyValue object is overwritten entirely. The content will
   *  need to be entirely re-parsed before it can be used
   */
  def dropKeyValueContent(): Unit = if (contentValid) {
    contentValid = false
    minimum = None
    maximum = None
    idaEncodedLeft = None
    idaEncodedRight = None
    idaEncodedContents = Map()
  }
  
  /** Called when an append operation is preformed on a KeyValue object we've already
   *  parsed.
   */
  def updateKeyValueContent(appendData: DataBuffer): Unit = if (contentValid) {
    KeyValueObjectStoreState.decodeUpdates(appendData) foreach { op => op match { 
      case op: SetMin => minimum = Some(op.value)
      case op: SetMax => maximum = Some(op.value)
      case op: SetLeft => idaEncodedLeft = Some(op.value)
      case op: SetRight => idaEncodedRight = Some(op.value)
      case op: Insert => 
        val key = Key(op.key)
        idaEncodedContents += (key -> Value(key, op.value, op.timestamp))
      case op: Delete => idaEncodedContents -= Key(op.value)
    }}
  }
}