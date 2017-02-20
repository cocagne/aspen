package com.ibm.aspen.core.network

import com.ibm.aspen.core.network.{protocol => P}
import com.google.flatbuffers.FlatBufferBuilder
import com.ibm.aspen.core.objects.ObjectRevision
import com.ibm.aspen.core.objects.ObjectRefcount
import com.ibm.aspen.core.ida.IDA
import com.ibm.aspen.core.ida.Replication
import com.ibm.aspen.core.ida.ReedSolomon
import com.ibm.aspen.core.objects.StorePointer
import com.ibm.aspen.core.objects.ObjectPointer
import java.util.UUID
import com.ibm.aspen.core.transaction.TransactionStatus
import com.ibm.aspen.core.transaction.UpdateType
import com.ibm.aspen.core.transaction.TransactionDisposition
import com.ibm.aspen.core.transaction.UpdateError
import com.ibm.aspen.core.transaction.DataUpdateOperation
import com.ibm.aspen.core.transaction.DataUpdate
import com.ibm.aspen.core.transaction.RefcountUpdate
import com.ibm.aspen.core.transaction.SerializedFinalizationAction
import com.ibm.aspen.core.transaction.TransactionDescription
import com.ibm.aspen.core.transaction.UpdateErrorResponse



object Codec {
  
  //-----------------------------------------------------------------------------------------------
  // Objects
  //
  def encode(builder:FlatBufferBuilder, u: UUID): Int = {
    P.UUID.createUUID(builder, u.getMostSignificantBits, u.getLeastSignificantBits)
  }
  def decode(o: P.UUID): UUID = {
    new UUID(o.mostSigBits(), o.leastSigBits())
  }
  
  
  def encode(builder:FlatBufferBuilder, rev:ObjectRevision): Int = {
    P.ObjectRevision.createObjectRevision(builder, rev.overwriteCount, rev.currentSize)
  }
  def decode(orev: P.ObjectRevision): ObjectRevision = {
    ObjectRevision(orev.overwriteCount(), orev.currentSize())
  }
  
  
  def encode(builder:FlatBufferBuilder, ref:ObjectRefcount): Int = {
    P.ObjectRefcount.createObjectRefcount(builder, ref.updateSerial, ref.count)
  }
  def decode(oref: P.ObjectRefcount): ObjectRefcount = {
    ObjectRefcount(oref.updateSerial(), oref.refcount())
  }
  
  
  def encode(builder:FlatBufferBuilder, o:StorePointer): Int = {
    val d = P.StorePointer.createDataVector(builder, o.data)
    P.StorePointer.createStorePointer(builder, o.poolIndex, d)
  }
  def decode(n: P.StorePointer): StorePointer = {
    val d = new Array[Byte](n.dataLength())
    n.dataAsByteBuffer().get(d)
    StorePointer(n.storeIndex(), d)
  }
  
  
  def encode(builder:FlatBufferBuilder, o:ObjectPointer): Int = {
    val uuid = encode(builder, o.uuid)
    val poolUUID = encode(builder, o.poolUUID)
    val ida = encode(builder, o.ida)
    val storePointers = P.ObjectPointer.createStorePointersVector(builder, o.storePointers.map(sp => encode(builder, sp)))
    P.ObjectPointer.startObjectPointer(builder)
    P.ObjectPointer.addUuid(builder, uuid)
    P.ObjectPointer.addPoolUuid(builder, poolUUID)
    o.size.foreach(s => P.ObjectPointer.addSize(builder, s))
    P.ObjectPointer.addIda(builder, ida)
    P.ObjectPointer.addStorePointers(builder, storePointers)
    P.ObjectPointer.endObjectPointer(builder)
  }
  def decode(n: P.ObjectPointer): ObjectPointer = {
    val uuid =  decode(n.uuid())
    val poolUUID = decode(n.poolUuid())
    val size = if (n.size() == 0) None else Some(n.size())
    
    val ida = if (n.idaType() == P.IDA.Replication)
      decode(n.ida(new P.Replication).asInstanceOf[P.Replication])
    else
      decode(n.ida(new P.ReedSolomon).asInstanceOf[P.ReedSolomon])
      
    def tolist(idx: Int, lst:List[StorePointer]): List[StorePointer] = {
      if (idx == -1) 
        lst 
      else 
        tolist(idx-1, decode(n.storePointers(idx)) :: lst)
    }
    
    val storePointers = tolist(n.storePointersLength()-1, Nil).toArray
    
    new ObjectPointer(uuid, poolUUID, size, ida, storePointers)
  }
  
  //-----------------------------------------------------------------------------------------------
  // IDA
  //
  def idaType(ida:IDA): Byte = ida match {
    case x: Replication => P.IDA.Replication
    case x: ReedSolomon => P.IDA.ReedSolomon
  }
  def encode(builder:FlatBufferBuilder, ida:IDA): Int = ida match {
    case x: Replication => P.Replication.createReplication(builder, x.width, x.writeThreshold)
    case x: ReedSolomon => P.ReedSolomon.createReedSolomon(builder, x.width, x.restoreThreshold, x.writeThreshold)
  }
  def decode(b:P.Replication): Replication = Replication(b.width(), b.writeThreshold())
  def decode(b:P.ReedSolomon): ReedSolomon = ReedSolomon(b.width(), b.readThreshold(), b.writeThreshold())
    
  
  //-----------------------------------------------------------------------------------------------
  // Transaction
  //
  def encodeTransactionStatus(e:TransactionStatus.Value): Byte = e match {
    case TransactionStatus.Unresolved => P.TransactionStatus.Unresolved
    case TransactionStatus.Committed  => P.TransactionStatus.Committed
    case TransactionStatus.Aborted    => P.TransactionStatus.Aborted
  }
  def decodeTransactionStatus(e: Byte): TransactionStatus.Value = e match {
    case P.TransactionStatus.Unresolved => TransactionStatus.Unresolved
    case P.TransactionStatus.Committed  => TransactionStatus.Committed
    case P.TransactionStatus.Aborted    => TransactionStatus.Aborted
  }
  
  
  def encodeUpdateType(e:UpdateType.Value): Byte = e match {
    case UpdateType.Data     => P.UpdateType.Data
    case UpdateType.Refcount => P.UpdateType.Refcount
  }
  def decodeUpdateType(e: Byte): UpdateType.Value = e match {
    case P.UpdateType.Data     => UpdateType.Data
    case P.UpdateType.Refcount => UpdateType.Refcount
  }
  
  
  def encodeTransactionDisposition(e:TransactionDisposition.Value): Byte = e match {
    case TransactionDisposition.Undetermined => P.TransactionDisposition.Undetermined
    case TransactionDisposition.Collision    => P.TransactionDisposition.Collision
    case TransactionDisposition.VoteCommit   => P.TransactionDisposition.VoteCommit
    case TransactionDisposition.VoteAbort    => P.TransactionDisposition.VoteAbort
  }
  def decodeTransactionDispositione(e: Byte): TransactionDisposition.Value = e match {
    case P.TransactionDisposition.Undetermined => TransactionDisposition.Undetermined
    case P.TransactionDisposition.Collision    => TransactionDisposition.Collision
    case P.TransactionDisposition.VoteCommit   => TransactionDisposition.VoteCommit
    case P.TransactionDisposition.VoteAbort    => TransactionDisposition.VoteAbort
  }
  
  
  def encodeUpdateError(e:UpdateError.Value): Byte = e match {
    case UpdateError.MissingUpdateData   => P.UpdateError.MissingUpdateData
    case UpdateError.ObjectMismatch      => P.UpdateError.ObjectMismatch
    case UpdateError.InvalidLocalPointer => P.UpdateError.InvalidLocalPointer
    case UpdateError.RevisionMismatch    => P.UpdateError.RevisionMismatch
    case UpdateError.RefcountMismatch    => P.UpdateError.RefcountMismatch
    case UpdateError.Collision           => P.UpdateError.Collision
    case UpdateError.CorruptedObject     => P.UpdateError.CorruptedObject
  }
  def decodeUpdateErrore(e: Byte): UpdateError.Value = e match {
    case P.UpdateError.MissingUpdateData   => UpdateError.MissingUpdateData
    case P.UpdateError.ObjectMismatch      => UpdateError.ObjectMismatch
    case P.UpdateError.InvalidLocalPointer => UpdateError.InvalidLocalPointer
    case P.UpdateError.RevisionMismatch    => UpdateError.RevisionMismatch
    case P.UpdateError.RefcountMismatch    => UpdateError.RefcountMismatch
    case P.UpdateError.Collision           => UpdateError.Collision
    case P.UpdateError.CorruptedObject     => UpdateError.CorruptedObject
  }
  
  
  def encodeDataUpdateOperation(e:DataUpdateOperation.Value): Byte = e match {
    case DataUpdateOperation.Append     => P.DataUpdateOperation.Append
    case DataUpdateOperation.Overwrite  => P.DataUpdateOperation.Overwrite
  }
  def decodeDataUpdateOperation(e: Byte): DataUpdateOperation.Value = e match {
    case P.DataUpdateOperation.Append     => DataUpdateOperation.Append
    case P.DataUpdateOperation.Overwrite  => DataUpdateOperation.Overwrite
  }
  
  
  def encode(builder:FlatBufferBuilder, o:DataUpdate): Int = {
    val optr = encode(builder, o.objectPointer)
    val rrev = encode(builder, o.requiredRevision)
    val op = encodeDataUpdateOperation(o.operation)
    
    P.DataUpdate.startDataUpdate(builder)
    P.DataUpdate.addObjectPointer(builder,optr)
    P.DataUpdate.addRequiredRevision(builder, rrev)
    P.DataUpdate.addOperation(builder, op)
    P.DataUpdate.endDataUpdate(builder)
  }
  def decode(n: P.DataUpdate): DataUpdate = {
    val optr =  decode(n.objectPointer())
    val rrev = decode(n.requiredRevision())
    val op = decodeDataUpdateOperation(n.operation())
    
    DataUpdate(optr, rrev, op)
  }
  
  
  def encode(builder:FlatBufferBuilder, o:RefcountUpdate): Int = {
    val optr = encode(builder, o.objectPointer)
    val rrc = encode(builder, o.requiredRefcount)
    val nrc = encode(builder, o.newRefcount)
    
    P.RefcountUpdate.startRefcountUpdate(builder)
    P.RefcountUpdate.addObjectPointer(builder,optr)
    P.RefcountUpdate.addRequiredRefcount(builder, rrc)
    P.RefcountUpdate.addNewRefcount(builder, nrc)
    P.RefcountUpdate.endRefcountUpdate(builder)
  }
  def decode(n: P.RefcountUpdate): RefcountUpdate = {
    val optr =  decode(n.objectPointer())
    val rrc = decode(n.requiredRefcount())
    val nrc = decode(n.newRefcount())
    
    RefcountUpdate(optr, rrc, nrc)
  }
  
  
  def encode(builder:FlatBufferBuilder, o:SerializedFinalizationAction): Int = {
    val uuid = encode(builder, o.typeUUID)
    val data = P.SerializedFinalizationAction.createDataVector(builder, o.data)
    P.SerializedFinalizationAction.startSerializedFinalizationAction(builder)
    P.SerializedFinalizationAction.addTypeUuid(builder, uuid)
    P.SerializedFinalizationAction.addData(builder, data)
    P.SerializedFinalizationAction.endSerializedFinalizationAction(builder)
  }
  def decode(n: P.SerializedFinalizationAction): SerializedFinalizationAction = {
    val uuid =  decode(n.typeUuid())
    val data = new Array[Byte](n.dataLength())
    n.dataAsByteBuffer().get(data)
    SerializedFinalizationAction(uuid, data)
  }
  
  
  def encode(builder:FlatBufferBuilder, o:TransactionDescription): Int = {
    val transactionUUID = encode(builder, o.transactionUUID)
    val primaryPoolUUID = encode(builder, o.primaryPoolUUID)
    val dataUpdates = P.TransactionDescription.createDataUpdatesVector(builder, o.dataUpdates.map(du => encode(builder, du)).toArray)
    val refcountUpdates = P.TransactionDescription.createRefcountUpdatesVector(builder, o.refcountUpdates.map(ru => encode(builder, ru)).toArray)
    val finalizationActions = P.TransactionDescription.createFinalizationActionsVector(builder, o.finalizationActions.map(fa => encode(builder, fa)).toArray)
    
    P.TransactionDescription.startTransactionDescription(builder)
    P.TransactionDescription.addTransactionUuid(builder, transactionUUID)
    P.TransactionDescription.addStartTimestamp(builder, o.startTimestamp)
    P.TransactionDescription.addPrimaryPoolUuid(builder, primaryPoolUUID)
    P.TransactionDescription.addDesignatedLeaderUid(builder, o.designatedLeaderUID)
    P.TransactionDescription.addDataUpdates(builder, dataUpdates)
    P.TransactionDescription.addRefcountUpdates(builder, refcountUpdates)
    P.TransactionDescription.addFinalizationActions(builder, finalizationActions)
    P.TransactionDescription.endTransactionDescription(builder)
  }
  def decode(n: P.TransactionDescription): TransactionDescription = {
    val transactionUUID = decode(n.transactionUuid())
    val startTimestamp = n.startTimestamp()
    val primaryPoolUUID = decode(n.primaryPoolUuid())
    val designatedLeaderUID = n.designatedLeaderUid()
      
    def dataUpdates(idx: Int, l:List[DataUpdate]): List[DataUpdate] = if (idx == -1) 
        l
      else 
        dataUpdates(idx-1, decode(n.dataUpdates(idx)) :: l)
    
    def refcountUpdates(idx: Int, l:List[RefcountUpdate]): List[RefcountUpdate] = if (idx == -1) 
        l
      else 
        refcountUpdates(idx-1, decode(n.refcountUpdates(idx)) :: l)
        
    def finalizationActions(idx: Int, l:List[SerializedFinalizationAction]): List[SerializedFinalizationAction] = if (idx == -1) 
        l
      else 
        finalizationActions(idx-1, decode(n.finalizationActions(idx)) :: l)
    
    TransactionDescription(
        transactionUUID,
        startTimestamp,
        primaryPoolUUID,
        designatedLeaderUID,
        dataUpdates(n.dataUpdatesLength()-1, Nil),
        refcountUpdates(n.refcountUpdatesLength()-1, Nil),
        finalizationActions(n.finalizationActionsLength()-1, Nil))
        
  }
  

  def encode(builder:FlatBufferBuilder, o:UpdateErrorResponse): Int = {
    val updateType = encodeUpdateType(o.updateType)
    val updateError = encodeUpdateError(o.updateError)
    val currentRev = encode(builder, o.currentRevision)
    val currentRef = encode(builder, o.currentRefcount)
    val collidingTx = encode(builder, o.conflictingTransaction)
    
    P.UpdateErrorResponse.startUpdateErrorResponse(builder)
    P.UpdateErrorResponse.addUpdateType(builder, updateType)
    P.UpdateErrorResponse.addUpdateIndex(builder, o.updateIndex)
    P.UpdateErrorResponse.addUpdateError(builder, updateError)
    P.UpdateErrorResponse.addCurrentRevision(builder, currentRev)
    P.UpdateErrorResponse.addCurrentRefcount(builder, currentRef)
    P.UpdateErrorResponse.addCollidingTransaction(builder, collidingTx)
    P.UpdateErrorResponse.endUpdateErrorResponse(builder)
  }
  def decode(n: P.UpdateErrorResponse): UpdateErrorResponse = {
    val updateType =  decodeUpdateType(n.updateType())
    val updateIndex = n.updateIndex()
    val updateError = decodeUpdateErrore(n.updateError())
    val currentRev = decode(n.currentRevision())
    val currentRef = decode(n.currentRefcount())
    val collidingTx = decode(n.collidingTransaction())
    
    UpdateErrorResponse(updateType, updateIndex, updateError, currentRev, currentRef, collidingTx)
  }
}