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
import com.ibm.aspen.core.transaction.paxos.ProposalID
import com.ibm.aspen.core.data_store.DataStoreID
import com.ibm.aspen.core.transaction.TxPrepare
import com.ibm.aspen.core.transaction.TxPrepareResponse
import com.ibm.aspen.core.transaction.TxAccept
import com.ibm.aspen.core.transaction.TxAcceptResponse
import com.ibm.aspen.core.transaction.TxFinalized
import com.ibm.aspen.core.allocation.Allocate
import com.ibm.aspen.core.allocation.AllocateResponse
import com.ibm.aspen.core.allocation.AllocationError
import com.ibm.aspen.core.read.Read
import com.ibm.aspen.core.read.ReadResponse
import com.ibm.aspen.core.read.ReadError
import java.nio.ByteBuffer



object Codec {
  
  //-----------------------------------------------------------------------------------------------
  // Objects
  //-----------------------------------------------------------------------------------------------
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
    val ida = encode(builder, o.ida)
    val storePointers = P.ObjectPointer.createStorePointersVector(builder, o.storePointers.map(sp => encode(builder, sp)))
    P.ObjectPointer.startObjectPointer(builder)
    P.ObjectPointer.addUuid(builder, encode(builder, o.uuid))
    P.ObjectPointer.addPoolUuid(builder, encode(builder, o.poolUUID))
    o.size.foreach(s => P.ObjectPointer.addSize(builder, s))
    P.ObjectPointer.addIdaType(builder, idaType(o.ida))
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
  
  def objectPointerToByteBuffer(o: ObjectPointer): ByteBuffer = {
    
    val builder = new FlatBufferBuilder(2048)
    
    val d = Codec.encode(builder, o)

    builder.finish(d)
    
    val db = builder.dataBuffer()
    
    val arr = new Array[Byte](db.capacity - db.position)
    db.get(arr)
    
    ByteBuffer.wrap(arr)
  }
  def byteBufferToObjectPointer(bb: ByteBuffer): ObjectPointer = {
    Codec.decode(P.ObjectPointer.getRootAsObjectPointer(bb))
  }
  
  //-----------------------------------------------------------------------------------------------
  // IDA
  //-----------------------------------------------------------------------------------------------
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
  // Transaction Description
  //-----------------------------------------------------------------------------------------------
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
    case TransactionDisposition.VoteCommit   => P.TransactionDisposition.VoteCommit
    case TransactionDisposition.VoteAbort    => P.TransactionDisposition.VoteAbort
  }
  def decodeTransactionDispositione(e: Byte): TransactionDisposition.Value = e match {
    case P.TransactionDisposition.Undetermined => TransactionDisposition.Undetermined
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
    val op = encodeDataUpdateOperation(o.operation)
    
    P.DataUpdate.startDataUpdate(builder)
    P.DataUpdate.addObjectPointer(builder,optr)
    P.DataUpdate.addRequiredRevision(builder, encode(builder, o.requiredRevision))
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
    
    P.RefcountUpdate.startRefcountUpdate(builder)
    P.RefcountUpdate.addObjectPointer(builder,optr)
    P.RefcountUpdate.addRequiredRefcount(builder, encode(builder, o.requiredRefcount))
    P.RefcountUpdate.addNewRefcount(builder, encode(builder, o.newRefcount))
    P.RefcountUpdate.endRefcountUpdate(builder)
  }
  def decode(n: P.RefcountUpdate): RefcountUpdate = {
    val optr =  decode(n.objectPointer())
    val rrc = decode(n.requiredRefcount())
    val nrc = decode(n.newRefcount())
    
    RefcountUpdate(optr, rrc, nrc)
  }
  
  
  def encode(builder:FlatBufferBuilder, o:SerializedFinalizationAction): Int = {
    val data = P.SerializedFinalizationAction.createDataVector(builder, o.data)
    P.SerializedFinalizationAction.startSerializedFinalizationAction(builder)
    P.SerializedFinalizationAction.addTypeUuid(builder, encode(builder, o.typeUUID))
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
    val primaryObject = encode(builder, o.primaryObject)
    val dataUpdates = P.TransactionDescription.createDataUpdatesVector(builder, o.dataUpdates.map(du => encode(builder, du)).toArray)
    val refcountUpdates = P.TransactionDescription.createRefcountUpdatesVector(builder, o.refcountUpdates.map(ru => encode(builder, ru)).toArray)
    val finalizationActions = P.TransactionDescription.createFinalizationActionsVector(builder, o.finalizationActions.map(fa => encode(builder, fa)).toArray)
    
    val originatingClient = o.originatingClient match {
      case None => -1
      case Some(client) => P.TransactionDescription.createOriginatingClientVector(builder, client.serialized)
    }
    
    P.TransactionDescription.startTransactionDescription(builder)
    P.TransactionDescription.addTransactionUuid(builder, encode(builder, o.transactionUUID))
    P.TransactionDescription.addStartTimestamp(builder, o.startTimestamp)
    P.TransactionDescription.addPrimaryObject(builder, primaryObject)
    P.TransactionDescription.addDesignatedLeaderUid(builder, o.designatedLeaderUID)
    P.TransactionDescription.addDataUpdates(builder, dataUpdates)
    P.TransactionDescription.addRefcountUpdates(builder, refcountUpdates)
    P.TransactionDescription.addFinalizationActions(builder, finalizationActions)
    if (originatingClient != -1)
      P.TransactionDescription.addOriginatingClient(builder, originatingClient)
    P.TransactionDescription.endTransactionDescription(builder)
  }
  def decode(n: P.TransactionDescription): TransactionDescription = {
    val transactionUUID = decode(n.transactionUuid())
    val startTimestamp = n.startTimestamp()
    val primaryObject = decode(n.primaryObject())
    val designatedLeaderUID = n.designatedLeaderUid()
    val originatingClient = if (n.originatingClientLength() == 0) 
      None 
    else {  
        val buf = new Array[Byte](n.originatingClientLength())
        n.originatingClientAsByteBuffer().get(buf)
        Some(Client(buf))
    }
      
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
        primaryObject,
        designatedLeaderUID,
        dataUpdates(n.dataUpdatesLength()-1, Nil),
        refcountUpdates(n.refcountUpdatesLength()-1, Nil),
        finalizationActions(n.finalizationActionsLength()-1, Nil),
        originatingClient)
  }
  
  //-----------------------------------------------------------------------------------------------
  // Transaction Messages
  //-----------------------------------------------------------------------------------------------
  
  def encode(builder:FlatBufferBuilder, o:ProposalID): Int = {
    P.ProposalID.createProposalID(builder, o.number, o.peer)
  }
  def decode(o: P.ProposalID): ProposalID = {
    ProposalID(o.number(), o.uid())
  }
  
  
  def encode(builder:FlatBufferBuilder, o:DataStoreID): Int = {
    P.DataStoreID.startDataStoreID(builder)
    P.DataStoreID.addStoragePoolUuid(builder, encode(builder, o.poolUUID))
    P.DataStoreID.addStoragePoolIndex(builder, o.poolIndex)
    P.DataStoreID.endDataStoreID(builder)
  }
  def decode(n: P.DataStoreID): DataStoreID = {
    val poolUUID = decode(n.storagePoolUuid())
    val poolIndex = n.storagePoolIndex()
    
    new DataStoreID(poolUUID, poolIndex)
  }
  
  
  def encode(builder:FlatBufferBuilder, o:UpdateErrorResponse): Int = {
    val updateType = encodeUpdateType(o.updateType)
    val updateError = encodeUpdateError(o.updateError)
    var collidingTx = -1
    
    if (o.conflictingTransaction.isDefined)
      collidingTx = encode(builder, o.conflictingTransaction.get)
    
    P.UpdateErrorResponse.startUpdateErrorResponse(builder)
    P.UpdateErrorResponse.addUpdateType(builder, updateType)
    P.UpdateErrorResponse.addUpdateIndex(builder, o.updateIndex)
    P.UpdateErrorResponse.addUpdateError(builder, updateError)
    if (o.currentRevision.isDefined)
      P.UpdateErrorResponse.addCurrentRevision(builder, encode(builder, o.currentRevision.get))
    if (o.currentRefcount.isDefined)
      P.UpdateErrorResponse.addCurrentRefcount(builder, encode(builder, o.currentRefcount.get))
    if (collidingTx != -1)
      P.UpdateErrorResponse.addCollidingTransaction(builder, collidingTx)
    P.UpdateErrorResponse.endUpdateErrorResponse(builder)
  }
  def decode(n: P.UpdateErrorResponse): UpdateErrorResponse = {
    val updateType =  decodeUpdateType(n.updateType())
    val updateIndex = n.updateIndex()
    val updateError = decodeUpdateErrore(n.updateError())
    val currentRev = if(n.currentRevision() == null) None else Some(decode(n.currentRevision()))
    val currentRef = if(n.currentRefcount() == null) None else Some(decode(n.currentRefcount()))
    val collidingTx = if(n.collidingTransaction() == null) None else Some(decode(n.collidingTransaction()))
    
    UpdateErrorResponse(updateType, updateIndex, updateError, currentRev, currentRef, collidingTx)
  }
  
  
  def encode(builder:FlatBufferBuilder, o:TxPrepare): Int = {
    val from = encode(builder, o.from)
    val txd = encode(builder, o.txd)
    
    P.TxPrepare.startTxPrepare(builder)
    P.TxPrepare.addFrom(builder, from)
    P.TxPrepare.addTxd(builder, txd)
    P.TxPrepare.addProposalId(builder, encode(builder, o.proposalId))
    P.TxPrepare.endTxPrepare(builder)
  }
  def decode(n: P.TxPrepare): TxPrepare = {
    val from = decode(n.from())
    val txd = decode(n.txd())
    val proposalId = decode(n.proposalId())
    
    TxPrepare(from, txd, proposalId)
  }
  
  
  def encode(builder:FlatBufferBuilder, o:TxPrepareResponse): Int = {
    val from = encode(builder, o.from) 
    var promisedId:ProposalID = null
    var lastAcceptedId:ProposalID = null 
    var lastAcceptedValue = false
    val responseType = o.response match {
      case Right(p) =>
        p.lastAccepted.foreach(tpl => {
          lastAcceptedId = tpl._1
          lastAcceptedValue = tpl._2
        })
        P.TxPrepareResponseType.Promise
      case Left(n) => 
        promisedId = n.promisedId
        P.TxPrepareResponseType.Nack
    }
    val disposition = encodeTransactionDisposition(o.disposition)
    val errors = P.TxPrepareResponse.createErrorsVector(builder, o.errors.map(ue => encode(builder, ue)).toArray)
    
    P.TxPrepareResponse.startTxPrepareResponse(builder)
    P.TxPrepareResponse.addFrom(builder, from)
    P.TxPrepareResponse.addTransactionUuid(builder, encode(builder, o.transactionUUID))
    P.TxPrepareResponse.addResponseType(builder, responseType)
    P.TxPrepareResponse.addProposalId(builder, encode(builder, o.proposalId))
    if (promisedId != null) P.TxPrepareResponse.addPromisedId(builder, encode(builder, promisedId))
    if (lastAcceptedId != null) {
      P.TxPrepareResponse.addLastAcceptedId(builder, encode(builder, lastAcceptedId))
      P.TxPrepareResponse.addLastAcceptedValue(builder, lastAcceptedValue)
    }
    P.TxPrepareResponse.addDisposition(builder, disposition)
    P.TxPrepareResponse.addErrors(builder, errors)
    P.TxPrepareResponse.endTxPrepareResponse(builder)
  }
  def decode(n: P.TxPrepareResponse): TxPrepareResponse = {
    val from = decode(n.from())
    val transactionUUID = decode(n.transactionUuid())
    val response = n.responseType() match {
      case P.TxPrepareResponseType.Promise =>
        val lastAcceptedId = decode(n.lastAcceptedId())
        val opt = if (lastAcceptedId == null) None else Some((lastAcceptedId, n.lastAcceptedValue()))
        Right(TxPrepareResponse.Promise(opt))
      case P.TxPrepareResponseType.Nack => Left(TxPrepareResponse.Nack(decode(n.promisedId())))
    }
    
    val proposalId = decode(n.proposalId())
    val disposition = decodeTransactionDispositione(n.disposition())
   
    def errors(idx: Int, l:List[UpdateErrorResponse]): List[UpdateErrorResponse] = if (idx == -1) 
        l
      else 
        errors(idx-1, decode(n.errors(idx)) :: l)
        
    TxPrepareResponse(from, transactionUUID, response, proposalId, disposition, errors(n.errorsLength()-1, Nil))
  }
  

  def encode(builder:FlatBufferBuilder, o:TxAccept): Int = {
    val from = encode(builder, o.from)
    
    P.TxAccept.startTxAccept(builder)
    P.TxAccept.addFrom(builder, from)
    P.TxAccept.addTransactionUuid(builder, encode(builder, o.transactionUUID))
    P.TxAccept.addProposalId(builder, encode(builder, o.proposalId))
    P.TxAccept.addValue(builder, o.value)
    P.TxAccept.endTxAccept(builder)
  }
  def decode(n: P.TxAccept): TxAccept = {
    val from = decode(n.from())
    val transactionUUID = decode(n.transactionUuid())
    val proposalId = decode(n.proposalId())
    val value = n.value()
    
    TxAccept(from, transactionUUID, proposalId, value)
  }
  
  
  def encode(builder:FlatBufferBuilder, o:TxAcceptResponse): Int = {
    val from = encode(builder, o.from)
    
    P.TxAcceptResponse.startTxAcceptResponse(builder)
    P.TxAcceptResponse.addFrom(builder, from)
    P.TxAcceptResponse.addTransactionUuid(builder, encode(builder, o.transactionUUID))
    P.TxAcceptResponse.addProposalId(builder, encode(builder, o.proposalId))
    
    o.response match {
      case Left(nack) => 
        P.TxAcceptResponse.addIsNack(builder, true)
        P.TxAcceptResponse.addPromisedId(builder, encode(builder, nack.promisedId))
        
      case Right(accepted) =>
        P.TxAcceptResponse.addIsNack(builder, false)
        P.TxAcceptResponse.addValue(builder, accepted.value)    
    }
    
    P.TxAcceptResponse.endTxAcceptResponse(builder)
  }
  def decode(n: P.TxAcceptResponse): TxAcceptResponse = {
    val from = decode(n.from())
    val transactionUUID = decode(n.transactionUuid())
    val proposalId = decode(n.proposalId())
    
    if (n.isNack())
      TxAcceptResponse(from, transactionUUID, proposalId, Left(TxAcceptResponse.Nack(decode(n.promisedId()))))
    else
      TxAcceptResponse(from, transactionUUID, proposalId, Right(TxAcceptResponse.Accepted(n.value())))
  }
  
  
  def encode(builder:FlatBufferBuilder, o:TxFinalized): Int = {
    val from = encode(builder, o.from)
    
    P.TxFinalized.startTxFinalized(builder)
    P.TxFinalized.addFrom(builder, from)
    P.TxFinalized.addTransactionUuid(builder, encode(builder, o.transactionUUID))
    P.TxFinalized.addCommitted(builder, o.committed)
    P.TxFinalized.endTxFinalized(builder)
  }
  def decode(n: P.TxFinalized): TxFinalized = {
    val from = decode(n.from())
    val transactionUUID = decode(n.transactionUuid())
    
    TxFinalized(from, transactionUUID, n.committed())
  }
  
  //-----------------------------------------------------------------------------------------------
  // Allocation Messages
  //-----------------------------------------------------------------------------------------------
  
  def encode(builder:FlatBufferBuilder, o:Allocate): Int = {
    val toStore = encode(builder, o.toStore)
    val clientData = P.Allocate.createFromClientVector(builder, o.fromClient.serialized)
    builder.createUnintializedVector(1, o.objectData.capacity, 1).put(o.objectData)
    o.objectData.position(0)
    val objectData = builder.endVector()
    val allocObj = encode(builder, o.allocatingObject)
    
    P.Allocate.startAllocate(builder)
    P.Allocate.addToStore(builder, toStore)
    P.Allocate.addFromClient(builder, clientData)
    P.Allocate.addNewObjectUUID(builder, encode(builder, o.newObjectUUID))
    o.objectSize.foreach( sz => P.Allocate.addObjectSize(builder, sz) )
    P.Allocate.addObjectData(builder, objectData)
    P.Allocate.addInitialRefcount(builder, encode(builder, o.initialRefcount))
    P.Allocate.addAllocationTransactionUUID(builder, encode(builder, o.allocationTransactionUUID))
    P.Allocate.addAllocatingObject(builder, allocObj)
    P.Allocate.addAllocatingObjectRevision(builder, encode(builder, o.allocatingObjectRevision))
    P.Allocate.endAllocate(builder)
  }
  def decode(n: P.Allocate): Allocate = {
    val toStore = decode(n.toStore())
    val fromClient = new Array[Byte](n.fromClientLength())
    n.fromClientAsByteBuffer().get(fromClient)
    
    val newObjectUUID = decode(n.newObjectUUID())
    val objectSize = if (n.objectSize() == 0) None else Some(n.objectSize())
    val objectData = ByteBuffer.allocateDirect(n.objectDataLength())
    objectData.put(n.objectDataAsByteBuffer())
    objectData.position(0)
    val initialRefcount = decode(n.initialRefcount())
    val allocationTransactionUUID = decode(n.allocationTransactionUUID())
    val allocatingObject = decode(n.allocatingObject())
    val allocatingObjectRevision = decode(n.allocatingObjectRevision())
    Allocate(toStore, Client(fromClient), newObjectUUID, objectSize, objectData, initialRefcount, 
        allocationTransactionUUID, allocatingObject, allocatingObjectRevision)
  }
  
  def encode(builder:FlatBufferBuilder, o:AllocateResponse): Int = {
    val fromStoreID = encode(builder, o.fromStoreId)
    val resultPointer = o.result match {
      case Right(sp) => encode(builder, sp)
      case Left(_) => -1
    }
    
    P.AllocateResponse.startAllocateResponse(builder)
    P.AllocateResponse.addFromStoreID(builder, fromStoreID)
    P.AllocateResponse.addAllocationTransactionUUID(builder, encode(builder, o.allocationTransactionUUID))
    o.result match {
      case Right(sp) => P.AllocateResponse.addResultPointer(builder, resultPointer)
      case Left(err) => P.AllocateResponse.addResultError(builder, err match {
        case AllocationError.InsufficientSpace => P.AllocationError.InsufficientSpace
      })
    }
    P.AllocateResponse.endAllocateResponse(builder)
  }
  def decode(n: P.AllocateResponse): AllocateResponse = {
    val fromStoreId = decode(n.fromStoreID())
    val allocationTransactionUUID = decode(n.allocationTransactionUUID())
    val result = if (n.resultPointer() == null) {
      n.resultError() match {
        case P.AllocationError.InsufficientSpace => Left(AllocationError.InsufficientSpace)
      }
    } else {
      Right(decode(n.resultPointer()))
    }
    AllocateResponse(fromStoreId, allocationTransactionUUID, result)
  }
  
  //-----------------------------------------------------------------------------------------------
  // Read Messages
  //-----------------------------------------------------------------------------------------------
  def encode(builder:FlatBufferBuilder, o:Read): Int = {
    val toStore = encode(builder, o.toStore)
    val clientData = P.Read.createFromClientVector(builder, o.fromClient.serialized)
    val optr = encode(builder, o.objectPointer)
    
    P.Read.startRead(builder)
    P.Read.addToStore(builder, toStore)
    P.Read.addFromClient(builder, clientData)
    P.Read.addReadUUID(builder, encode(builder, o.readUUID))
    P.Read.addObjectPointer(builder, optr)
    P.Read.addReturnObjectData(builder, o.returnObjectData)
    P.Read.addReturnLockedTransaction(builder, o.returnLockedTransaction)
    P.Read.endRead(builder)
  }
  def decode(n: P.Read): Read = {
    val toStore = decode(n.toStore())
    val fromClient = new Array[Byte](n.fromClientLength())
    n.fromClientAsByteBuffer().get(fromClient)
    val readUUID = decode(n.readUUID())
    val objectPointer = decode(n.objectPointer())
    val returnObjectData = n.returnObjectData()
    val returnLockedTransaction = n.returnLockedTransaction()
    
    Read(toStore, Client(fromClient), readUUID, objectPointer, returnObjectData, returnLockedTransaction)
  }
  
  def encode(builder:FlatBufferBuilder, o:ReadResponse): Int = {
    val fromStore = encode(builder, o.fromStore)
    
    val (objectData, lockedTransaction) = o.result match {
      case Left(_) => (-1, -1)
      case Right(cs) => 
        val od = cs.objectData match {
          case None => -1
          case Some(d) =>
            builder.createUnintializedVector(1, d.capacity, 1).put(d)
            d.position(0)
            builder.endVector()
        }
        val td = cs.lockedTransaction match {
          case None => -1
          case Some(txd) => encode(builder, txd)
        }
        (od, td)
    }
    
    P.ReadResponse.startReadResponse(builder)
    P.ReadResponse.addFromStore(builder, fromStore)
    P.ReadResponse.addReadUUID(builder, encode(builder, o.readUUID))
    o.result match {
      case Left(err) => 
        val readError = err match {
          case ReadError.ObjectMismatch => P.ReadError.ObjectMismatch
          case ReadError.InvalidLocalPointer => P.ReadError.InvalidLocalPointer
          case ReadError.CorruptedObject => P.ReadError.CorruptedObject
          case ReadError.UnexpectedInternalError => P.ReadError.UnexpectedInternalError
        }
        P.ReadResponse.addReadError(builder, readError)
      case Right(cs) =>
        P.ReadResponse.addRevision(builder, encode(builder, cs.revision))
        P.ReadResponse.addRefcount(builder, encode(builder, cs.refcount))
        if (objectData != -1) P.ReadResponse.addObjectData(builder, objectData)
        if (lockedTransaction != -1) P.ReadResponse.addLockedTransaction(builder, lockedTransaction)
    }
    P.ReadResponse.endReadResponse(builder)
  }
  def decode(n: P.ReadResponse): ReadResponse = {
    val fromStore = decode(n.fromStore())
    val readUUID = decode(n.readUUID())
    val result = if (n.revision() == null) {
      n.readError() match {
        case P.ReadError.ObjectMismatch => Left(ReadError.ObjectMismatch)
        case P.ReadError.InvalidLocalPointer => Left(ReadError.InvalidLocalPointer)
        case P.ReadError.CorruptedObject => Left(ReadError.CorruptedObject)
        case P.ReadError.UnexpectedInternalError => Left(ReadError.UnexpectedInternalError)
      }
    } else {
      val revision = decode(n.revision())
      val refcount = decode(n.refcount())
      val objectData = if (n.objectDataLength() <= 0) None else {
        val buff = ByteBuffer.allocateDirect(n.objectDataLength())
        buff.put(n.objectDataAsByteBuffer())
        buff.position(0)
        Some(buff)
      }
      val lockedTransaction = if (n.lockedTransaction() == null) None else { Some(decode(n.lockedTransaction())) }
      Right(ReadResponse.CurrentState(revision, refcount, objectData, lockedTransaction))
    }
    ReadResponse(fromStore, readUUID, result)
  }
  
}