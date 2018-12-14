package com.ibm.aspen.core.network

import java.nio.charset.StandardCharsets
import java.nio.{ByteBuffer, ByteOrder}
import java.util.UUID

import com.google.flatbuffers.FlatBufferBuilder
import com.ibm.aspen.core.{DataBuffer, HLCTimestamp}
import com.ibm.aspen.core.allocation._
import com.ibm.aspen.core.data_store._
import com.ibm.aspen.core.ida.{IDA, ReedSolomon, Replication}
import com.ibm.aspen.core.network.{protocol => P}
import com.ibm.aspen.core.objects._
import com.ibm.aspen.core.objects.keyvalue._
import com.ibm.aspen.core.read._
import com.ibm.aspen.core.transaction._
import com.ibm.aspen.core.transaction.paxos.ProposalID



object NetworkCodec {
  
  //-----------------------------------------------------------------------------------------------
  // Objects
  //-----------------------------------------------------------------------------------------------
  def encode(builder:FlatBufferBuilder, u: UUID): Int = {
    P.UUID.createUUID(builder, u.getMostSignificantBits, u.getLeastSignificantBits)
  }
  def decode(o: P.UUID): UUID = {
    new UUID(o.mostSigBits(), o.leastSigBits())
  }
  
  
  def encodeObjectRevision(builder:FlatBufferBuilder, rev:ObjectRevision): Int = {
    P.ObjectRevision.createObjectRevision(builder, rev.lastUpdateTxUUID.getMostSignificantBits, rev.lastUpdateTxUUID.getLeastSignificantBits)
  }
  def decode(o: P.ObjectRevision): ObjectRevision = {
    ObjectRevision(new UUID(o.mostSigBits(), o.leastSigBits()))
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
    o match {
      case d: DataObjectPointer => P.ObjectPointer.addObjectType(builder, P.ObjectType.Data)
      case k: KeyValueObjectPointer => P.ObjectPointer.addObjectType(builder, P.ObjectType.KeyValue)
    }
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
    
    n.objectType() match {
      case P.ObjectType.Data => new DataObjectPointer(uuid, poolUUID, size, ida, storePointers)
      case P.ObjectType.KeyValue => new KeyValueObjectPointer(uuid, poolUUID, size, ida, storePointers)
    }
  }
  
  def objectPointerToByteBuffer(o: ObjectPointer): ByteBuffer = ByteBuffer.wrap(objectPointerToByteArray(o))
  def byteBufferToObjectPointer(bb: ByteBuffer): ObjectPointer = {
    NetworkCodec.decode(P.ObjectPointer.getRootAsObjectPointer(bb))
  }
  
  def byteBufferToArray(bb: ByteBuffer): Array[Byte] = {
    val arr = new Array[Byte](bb.limit() - bb.position())
    val ro = bb.asReadOnlyBuffer()
    ro.get(arr)
    arr
  }
  
  def objectPointerToByteArray(o: ObjectPointer): Array[Byte] = {
    
    val builder = new FlatBufferBuilder(2048)
    
    val d = NetworkCodec.encode(builder, o)

    builder.finish(d)
    
    val db = builder.dataBuffer()
    
    val arr = new Array[Byte](db.limit() - db.position())
    db.get(arr)
    
    arr
  }
  def byteArrayToObjectPointer(arr: Array[Byte]): ObjectPointer = byteBufferToObjectPointer(ByteBuffer.wrap(arr))
  
  
  def encode(builder:FlatBufferBuilder, o:AllocationObjectStatus): Int = {
    val locksOffset = o.state match {
      case Left(_) => -1
      case Right(state) => if (state.locks.isEmpty) -1 else {
        P.AllocationObjectStatus.createLocksVector(builder, state.locks.map(ue => encode(builder, ue)).toArray)
      }
    }
    
    P.AllocationObjectStatus.startAllocationObjectStatus(builder)
    P.AllocationObjectStatus.addObjectUUID(builder, encode(builder, o.uuid))
    o.state match {
      case Right(state) =>
        P.AllocationObjectStatus.addRevision(builder, encodeObjectRevision(builder, state.revision))
        P.AllocationObjectStatus.addRefcount(builder, encode(builder, state.refcount))
        
        if (!state.locks.isEmpty)
          P.AllocationObjectStatus.addLocks(builder, locksOffset)
        
      case Left(err) =>
        P.AllocationObjectStatus.addReadError(builder, encodeReadError(err))
        
    }
    P.AllocationObjectStatus.endAllocationObjectStatus(builder)
  }
  def decode(n: P.AllocationObjectStatus): AllocationObjectStatus = {
    val objUUID = decode(n.objectUUID())
    
    if (n.revision() != null) {
      val rev = decode(n.revision())
      val ref = decode(n.refcount())
      
      def locks(idx: Int, l:List[Lock]): List[Lock] = if (idx == -1) 
        l
      else 
        locks(idx-1, decode(n.locks(idx)) :: l)
    
      AllocationObjectStatus(objUUID, Right(AllocationObjectStatus.State(rev, ref, locks(n.locksLength()-1, Nil))))
    } else {
      AllocationObjectStatus(objUUID, Left(decodeReadError(n.readError()))) 
    }
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
    case UpdateError.MissingUpdateData        => P.UpdateError.MissingUpdateData
    case UpdateError.ObjectMismatch           => P.UpdateError.ObjectMismatch
    case UpdateError.InvalidLocalPointer      => P.UpdateError.InvalidLocalPointer
    case UpdateError.RevisionMismatch         => P.UpdateError.RevisionMismatch
    case UpdateError.RefcountMismatch         => P.UpdateError.RefcountMismatch
    case UpdateError.TransactionCollision     => P.UpdateError.TransactionCollision
    case UpdateError.RebuildCollision         => P.UpdateError.RebuildCollision
    case UpdateError.CorruptedObject          => P.UpdateError.CorruptedObject
    case UpdateError.InsufficientFreeSpace    => P.UpdateError.InsufficientFreeSpace
    case UpdateError.InvalidObjectType        => P.UpdateError.InvalidObjectType
    case UpdateError.KeyValueRequirementError => P.UpdateError.KeyValueRequirementError
    case UpdateError.TransactionTimestampError => P.UpdateError.TransactionTimestampError
  }
  def decodeUpdateErrore(e: Byte): UpdateError.Value = e match {
    case P.UpdateError.MissingUpdateData        => UpdateError.MissingUpdateData
    case P.UpdateError.ObjectMismatch           => UpdateError.ObjectMismatch
    case P.UpdateError.InvalidLocalPointer      => UpdateError.InvalidLocalPointer
    case P.UpdateError.RevisionMismatch         => UpdateError.RevisionMismatch
    case P.UpdateError.RefcountMismatch         => UpdateError.RefcountMismatch
    case P.UpdateError.TransactionCollision     => UpdateError.TransactionCollision
    case P.UpdateError.RebuildCollision         => UpdateError.RebuildCollision
    case P.UpdateError.CorruptedObject          => UpdateError.CorruptedObject
    case P.UpdateError.InsufficientFreeSpace    => UpdateError.InsufficientFreeSpace
    case P.UpdateError.InvalidObjectType        => UpdateError.InvalidObjectType
    case P.UpdateError.KeyValueRequirementError => UpdateError.KeyValueRequirementError
    case P.UpdateError.TransactionTimestampError => UpdateError.TransactionTimestampError
  }
  
  
  def encodeDataUpdateOperation(e:DataUpdateOperation.Value): Byte = e match {
    case DataUpdateOperation.Append     => P.DataUpdateOperation.Append
    case DataUpdateOperation.Overwrite  => P.DataUpdateOperation.Overwrite
  }
  def decodeDataUpdateOperation(e: Byte): DataUpdateOperation.Value = e match {
    case P.DataUpdateOperation.Append     => DataUpdateOperation.Append
    case P.DataUpdateOperation.Overwrite  => DataUpdateOperation.Overwrite
  }
  
  
  def encodeKeyValueUpdateType(e:KeyValueUpdate.UpdateType.Value): Byte = e match {
    case KeyValueUpdate.UpdateType.Update    => P.KeyValueUpdateType.Update
  }
  def decodeKeyValueUpdateType(e: Byte): KeyValueUpdate.UpdateType.Value = e match {
    case P.KeyValueUpdateType.Update     => KeyValueUpdate.UpdateType.Update
  }
  
  def encodeKeyValueTimestampRequirementEnum(e:KeyValueUpdate.TimestampRequirement.Value): Byte = e match {
    case KeyValueUpdate.TimestampRequirement.Equals       => P.KeyValueTimestampRequirementEnum.Equals
    case KeyValueUpdate.TimestampRequirement.LessThan     => P.KeyValueTimestampRequirementEnum.LessThan
    case KeyValueUpdate.TimestampRequirement.Exists       => P.KeyValueTimestampRequirementEnum.Exists
    case KeyValueUpdate.TimestampRequirement.DoesNotExist => P.KeyValueTimestampRequirementEnum.DoesNotExist
  }
  def decodeKeyValueTimestampRequirementEnum(e: Byte): KeyValueUpdate.TimestampRequirement.Value = e match {
    case P.KeyValueTimestampRequirementEnum.Equals       => KeyValueUpdate.TimestampRequirement.Equals
    case P.KeyValueTimestampRequirementEnum.LessThan     => KeyValueUpdate.TimestampRequirement.LessThan
    case P.KeyValueTimestampRequirementEnum.Exists       => KeyValueUpdate.TimestampRequirement.Exists
    case P.KeyValueTimestampRequirementEnum.DoesNotExist => KeyValueUpdate.TimestampRequirement.DoesNotExist
  }
  
  
  def encode(builder:FlatBufferBuilder, o:DataUpdate): Int = {
    val optr = encode(builder, o.objectPointer)
    val op = encodeDataUpdateOperation(o.operation)
    
    P.DataUpdate.startDataUpdate(builder)
    P.DataUpdate.addObjectPointer(builder,optr)
    P.DataUpdate.addRequiredRevision(builder, encodeObjectRevision(builder, o.requiredRevision))
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
  
  
  def encode(builder:FlatBufferBuilder, o:VersionBump): Int = {
    val optr = encode(builder, o.objectPointer)
    
    P.VersionBump.startVersionBump(builder)
    P.VersionBump.addObjectPointer(builder,optr)
    P.VersionBump.addRequiredRevision(builder, encodeObjectRevision(builder, o.requiredRevision))
    P.VersionBump.endVersionBump(builder)
  }
  def decode(n: P.VersionBump): VersionBump = {
    val optr =  decode(n.objectPointer())
    val rrev = decode(n.requiredRevision())
    
    VersionBump(optr, rrev)
  }
  
  
  def encode(builder:FlatBufferBuilder, o:RevisionLock): Int = {
    val optr = encode(builder, o.objectPointer)
    
    P.RevisionLock.startRevisionLock(builder)
    P.RevisionLock.addObjectPointer(builder,optr)
    P.RevisionLock.addRequiredRevision(builder, encodeObjectRevision(builder, o.requiredRevision))
    P.RevisionLock.endRevisionLock(builder)
  }
  def decode(n: P.RevisionLock): RevisionLock = {
    val optr =  decode(n.objectPointer())
    val rrev = decode(n.requiredRevision())
    
    RevisionLock(optr, rrev)
  }
  
  
  def encode(builder:FlatBufferBuilder, o:KeyValueUpdate.KVRequirement): Int = {
    val key = P.KVReq.createKeyVector(builder, o.key.bytes)
    P.KVReq.startKVReq(builder)
    P.KVReq.addTsRequirement(builder, encodeKeyValueTimestampRequirementEnum(o.tsRequirement))
    P.KVReq.addTimestamp(builder, o.timestamp.asLong)
    P.KVReq.addKey(builder, key)
    P.KVReq.endKVReq(builder)
  }
  def decode(n: P.KVReq): KeyValueUpdate.KVRequirement = {
    val tsRequirement =  decodeKeyValueTimestampRequirementEnum(n.tsRequirement())
    val key = new Array[Byte](n.keyLength())
    val timestamp = HLCTimestamp(n.timestamp())
    n.keyAsByteBuffer().get(key)
    KeyValueUpdate.KVRequirement(Key(key), timestamp, tsRequirement)
  }
  
  
  def encode(builder:FlatBufferBuilder, o:KeyValueUpdate): Int = {
    val objectPointer = encode(builder, o.objectPointer)
    val requirements = P.KeyValueUpdate.createRequirementsVector(builder, o.requirements.map(r => encode(builder, r)).toArray)
    
    P.KeyValueUpdate.startKeyValueUpdate(builder)
    P.KeyValueUpdate.addObjectPointer(builder, objectPointer)
    P.KeyValueUpdate.addUpdateType(builder, encodeKeyValueUpdateType(o.updateType))
    o.requiredRevision.foreach { rr => P.KeyValueUpdate.addRequiredRevision(builder, encodeObjectRevision(builder, rr)) } 
    P.KeyValueUpdate.addRequirements(builder, requirements)
    P.KeyValueUpdate.addTimestamp(builder, o.timestamp.asLong)
    P.KeyValueUpdate.endKeyValueUpdate(builder)
  }
  def decode(n: P.KeyValueUpdate): KeyValueUpdate = {
    val timestamp = HLCTimestamp(n.timestamp())
    val objectPointer = decode(n.objectPointer())
    val updateType = decodeKeyValueUpdateType(n.updateType())
    val requiredRevision = if (n.requiredRevision() == null) None else Some(decode(n.requiredRevision()))
    
    def requirements(idx: Int, l:List[KeyValueUpdate.KVRequirement]): List[KeyValueUpdate.KVRequirement] = if (idx == -1)
        l
      else
        requirements(idx-1, decode(n.requirements(idx)) :: l)
        
    KeyValueUpdate(objectPointer.asInstanceOf[KeyValueObjectPointer], updateType, requiredRevision, requirements(n.requirementsLength()-1, Nil), timestamp)
  }
  
  
  def encode(builder:FlatBufferBuilder, o:TransactionRequirement): Int = {
    val offset = o match {
      case du: DataUpdate => encode(builder, du)
      case ru: RefcountUpdate => encode(builder, ru)
      case vb: VersionBump => encode(builder, vb)
      case rl: RevisionLock => encode(builder, rl)
      case kv: KeyValueUpdate => encode(builder, kv)
    }
    
    P.TransactionRequirement.startTransactionRequirement(builder)
    o match {
      case _: DataUpdate => P.TransactionRequirement.addDataUpdate(builder, offset)
      case _: RefcountUpdate => P.TransactionRequirement.addRefcountUpdate(builder, offset)
      case _: VersionBump => P.TransactionRequirement.addVersionBump(builder, offset)
      case _: RevisionLock => P.TransactionRequirement.addRevisionLock(builder, offset)
      case _: KeyValueUpdate => P.TransactionRequirement.addKvUpdate(builder, offset)
    }
    P.TransactionRequirement.endTransactionRequirement(builder)
  }
  def decode(n: P.TransactionRequirement): TransactionRequirement = {
    if (n.dataUpdate() != null)
      decode(n.dataUpdate())
    else if (n.refcountUpdate() != null)
      decode(n.refcountUpdate())
    else if (n.versionBump() != null)
      decode(n.versionBump())
    else if (n.revisionLock() != null)
      decode(n.revisionLock())
    else if (n.kvUpdate() != null)
      decode(n.kvUpdate())
    else
      throw new EncodingError("Unknown Transaction Requirement")
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
    val requirements = P.TransactionDescription.createRequirementsVector(builder, o.requirements.map(r => encode(builder, r)).toArray)
    val finalizationActions = P.TransactionDescription.createFinalizationActionsVector(builder, o.finalizationActions.map(fa => encode(builder, fa)).toArray)

    val notes = if (o.notes.isEmpty) -1 else {
      val count = o.notes.length
      val encoded = o.notes.map(s => s.getBytes(StandardCharsets.UTF_8))
      val esz = encoded.foldLeft(0)((sz, arr) => sz + arr.length)

      val arr = new Array[Byte](4 + 4*count + esz)
      val bb = ByteBuffer.wrap(arr)
      bb.putInt(count)
      encoded.foreach(arr => bb.putInt(arr.length))
      encoded.foreach(bb.put)

      P.TransactionDescription.createNotesVector(builder, arr)
    }
    
    val originatingClient = o.originatingClient match {
      case None => -1
      case Some(client) => P.TransactionDescription.createOriginatingClientVector(builder, client.serialized)
    }
    
    val notifyOnResolution = if (o.notifyOnResolution.isEmpty) -1 else {
      P.TransactionDescription.createNotifyOnResolutionVector(builder, o.notifyOnResolution.map(n => encode(builder, n)).toArray)
    }
    
    P.TransactionDescription.startTransactionDescription(builder)
    P.TransactionDescription.addTransactionUuid(builder, encode(builder, o.transactionUUID))
    P.TransactionDescription.addStartTimestamp(builder, o.startTimestamp)
    P.TransactionDescription.addPrimaryObject(builder, primaryObject)
    P.TransactionDescription.addDesignatedLeaderUid(builder, o.designatedLeaderUID)
    P.TransactionDescription.addRequirements(builder, requirements)
    P.TransactionDescription.addFinalizationActions(builder, finalizationActions)
    if (originatingClient != -1)
      P.TransactionDescription.addOriginatingClient(builder, originatingClient)
    if (notifyOnResolution != -1)
      P.TransactionDescription.addNotifyOnResolution(builder, notifyOnResolution)
    if (notes != -1)
      P.TransactionDescription.addNotes(builder, notes)
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
        Some(ClientID(buf))
    }

    val notes = if (n.notesLength() == 0) Nil else {
      val bb = n.notesAsByteBuffer()
      val count = bb.getInt()
      val sizes = (0 until count).map( _ => bb.getInt()).toList
      sizes.map { sz =>
        val arr = new Array[Byte](sz)
        bb.get(arr)
        new String(arr, StandardCharsets.UTF_8)
      }
    }
    
    def requirements(idx: Int, l:List[TransactionRequirement]): List[TransactionRequirement] = if (idx == -1)
        l
      else
        requirements(idx-1, decode(n.requirements(idx)) :: l)
        
    def finalizationActions(idx: Int, l:List[SerializedFinalizationAction]): List[SerializedFinalizationAction] = if (idx == -1) 
        l
      else 
        finalizationActions(idx-1, decode(n.finalizationActions(idx)) :: l)
        
    def notifyOnResolution(idx: Int, l:List[DataStoreID]): List[DataStoreID] = if (idx == -1)
        l
      else
        notifyOnResolution(idx-1, decode(n.notifyOnResolution(idx)) :: l)
    
    TransactionDescription(
        transactionUUID,
        startTimestamp,
        primaryObject,
        designatedLeaderUID,
        requirements(n.requirementsLength()-1, Nil),
        finalizationActions(n.finalizationActionsLength()-1, Nil),
        originatingClient,
        notifyOnResolution(n.notifyOnResolutionLength()-1, Nil),
        notes)
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
    val updateError = encodeUpdateError(o.updateError)
    var collidingTx = o.conflictingTransaction match {
      case None => -1
      case Some(t) =>
        val arr = new Array[Byte](16 + 8)
        val bb = ByteBuffer.wrap(arr)
        bb.order(ByteOrder.BIG_ENDIAN)
        bb.putLong(t._1.getMostSignificantBits)
        bb.putLong(t._1.getLeastSignificantBits)
        bb.putLong(t._2.asLong)
        P.UpdateErrorResponse.createCollidingTransactionVector(builder, arr)
    }
    
    P.UpdateErrorResponse.startUpdateErrorResponse(builder)
    P.UpdateErrorResponse.addObjectUuid(builder, encode(builder, o.objectUUID))
    P.UpdateErrorResponse.addUpdateError(builder, updateError)
    if (o.currentRevision.isDefined)
      P.UpdateErrorResponse.addCurrentRevision(builder, encodeObjectRevision(builder, o.currentRevision.get))
    if (o.currentRefcount.isDefined)
      P.UpdateErrorResponse.addCurrentRefcount(builder, encode(builder, o.currentRefcount.get))
    if (collidingTx != -1)
      P.UpdateErrorResponse.addCollidingTransaction(builder, collidingTx)
    P.UpdateErrorResponse.endUpdateErrorResponse(builder)
  }
  def decode(n: P.UpdateErrorResponse): UpdateErrorResponse = {
    val objectUUID = decode(n.objectUuid())
    val updateError = decodeUpdateErrore(n.updateError())
    val currentRev = if(n.currentRevision() == null) None else Some(decode(n.currentRevision()))
    val currentRef = if(n.currentRefcount() == null) None else Some(decode(n.currentRefcount()))
    
    val collidingTx = n.collidingTransactionAsByteBuffer() match {
      case null => None
      case bb =>
        bb.order(ByteOrder.BIG_ENDIAN)
        val msb = bb.getLong()
        val lsb = bb.getLong()
        val ts = bb.getLong()
        Some((new UUID(msb, lsb), HLCTimestamp(ts)))
    }
    
    UpdateErrorResponse(objectUUID, updateError, currentRev, currentRef, collidingTx)
  }
  
  
  def encode(builder:FlatBufferBuilder, o:TxPrepare): Int = {
    val to = encode(builder, o.to)
    val from = encode(builder, o.from)
    val txd = encode(builder, o.txd)
    
    P.TxPrepare.startTxPrepare(builder)
    P.TxPrepare.addTo(builder, to)
    P.TxPrepare.addFrom(builder, from)
    P.TxPrepare.addTxd(builder, txd)
    P.TxPrepare.addProposalId(builder, encode(builder, o.proposalId))
    P.TxPrepare.endTxPrepare(builder)
  }
  def decode(n: P.TxPrepare): TxPrepare = {
    val to = decode(n.to())
    val from = decode(n.from())
    val txd = decode(n.txd())
    val proposalId = decode(n.proposalId())
    
    TxPrepare(to, from, txd, proposalId)
  }
  
  
  def encode(builder:FlatBufferBuilder, o:TxPrepareResponse): Int = {
    val to = encode(builder, o.to)
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
    P.TxPrepareResponse.addTo(builder, to)
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
    val to = decode(n.to())
    val from = decode(n.from())
    val transactionUUID = decode(n.transactionUuid())
    val response = n.responseType() match {
      case P.TxPrepareResponseType.Promise =>
        val opt = if (n.lastAcceptedId() == null) None else Some((decode(n.lastAcceptedId()), n.lastAcceptedValue()))
        Right(TxPrepareResponse.Promise(opt))
      case P.TxPrepareResponseType.Nack => Left(TxPrepareResponse.Nack(decode(n.promisedId())))
    }
    
    val proposalId = decode(n.proposalId())
    val disposition = decodeTransactionDispositione(n.disposition())
   
    def errors(idx: Int, l:List[UpdateErrorResponse]): List[UpdateErrorResponse] = if (idx == -1) 
        l
      else 
        errors(idx-1, decode(n.errors(idx)) :: l)
        
    TxPrepareResponse(to, from, transactionUUID, response, proposalId, disposition, errors(n.errorsLength()-1, Nil))
  }
  

  def encode(builder:FlatBufferBuilder, o:TxAccept): Int = {
    val to = encode(builder, o.to)
    val from = encode(builder, o.from)
    
    P.TxAccept.startTxAccept(builder)
    P.TxAccept.addTo(builder, to)
    P.TxAccept.addFrom(builder, from)
    P.TxAccept.addTransactionUuid(builder, encode(builder, o.transactionUUID))
    P.TxAccept.addProposalId(builder, encode(builder, o.proposalId))
    P.TxAccept.addValue(builder, o.value)
    P.TxAccept.endTxAccept(builder)
  }
  def decode(n: P.TxAccept): TxAccept = {
    val to = decode(n.to())
    val from = decode(n.from())
    val transactionUUID = decode(n.transactionUuid())
    val proposalId = decode(n.proposalId())
    val value = n.value()
    
    TxAccept(to, from, transactionUUID, proposalId, value)
  }
  
  
  def encode(builder:FlatBufferBuilder, o:TxAcceptResponse): Int = {
    val to = encode(builder, o.to)
    val from = encode(builder, o.from)
    
    P.TxAcceptResponse.startTxAcceptResponse(builder)
    P.TxAcceptResponse.addTo(builder, to)
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
    val to = decode(n.to())
    val from = decode(n.from())
    val transactionUUID = decode(n.transactionUuid())
    val proposalId = decode(n.proposalId())
    
    if (n.isNack())
      TxAcceptResponse(to, from, transactionUUID, proposalId, Left(TxAcceptResponse.Nack(decode(n.promisedId()))))
    else
      TxAcceptResponse(to, from, transactionUUID, proposalId, Right(TxAcceptResponse.Accepted(n.value())))
  }
  
  
  def encode(builder:FlatBufferBuilder, o:TxResolved): Int = {
    val to = encode(builder, o.to)
    val from = encode(builder, o.from)
    
    P.TxResolved.startTxResolved(builder)
    P.TxResolved.addTo(builder, to)
    P.TxResolved.addFrom(builder, from)
    P.TxResolved.addTransactionUuid(builder, encode(builder, o.transactionUUID))
    P.TxResolved.addCommitted(builder, o.committed)
    P.TxResolved.endTxResolved(builder)
  }
  def decode(n: P.TxResolved): TxResolved = {
    val to = decode(n.to())
    val from = decode(n.from())
    val transactionUUID = decode(n.transactionUuid())
    
    TxResolved(to, from, transactionUUID, n.committed())
  }
  
  
  def encode(builder:FlatBufferBuilder, o:TxCommitted): Int = {
    val to = encode(builder, o.to)
    val from = encode(builder, o.from)
    
    P.TxCommitted.startTxCommitted(builder)
    P.TxCommitted.addTo(builder, to)
    P.TxCommitted.addFrom(builder, from)
    P.TxCommitted.addTransactionUuid(builder, encode(builder, o.transactionUUID))
    P.TxCommitted.endTxCommitted(builder)
  }
  def decode(n: P.TxCommitted): TxCommitted = {
    val to = decode(n.to())
    val from = decode(n.from())
    val transactionUUID = decode(n.transactionUuid())
    
    TxCommitted(to, from, transactionUUID)
  }
  
  
  def encode(builder:FlatBufferBuilder, o:TxFinalized): Int = {
    val to = encode(builder, o.to)
    val from = encode(builder, o.from)
    
    P.TxFinalized.startTxFinalized(builder)
    P.TxFinalized.addTo(builder, to)
    P.TxFinalized.addFrom(builder, from)
    P.TxFinalized.addTransactionUuid(builder, encode(builder, o.transactionUUID))
    P.TxFinalized.addCommitted(builder, o.committed)
    P.TxFinalized.endTxFinalized(builder)
  }
  def decode(n: P.TxFinalized): TxFinalized = {
    val to = decode(n.to())
    val from = decode(n.from())
    val transactionUUID = decode(n.transactionUuid())
    
    TxFinalized(to, from, transactionUUID, n.committed())
  }
  
  def encode(builder:FlatBufferBuilder, o:TxHeartbeat): Int = {
    val to = encode(builder, o.to)
    val from = encode(builder, o.from)
    
    P.TxHeartbeat.startTxHeartbeat(builder)
    P.TxHeartbeat.addTo(builder, to)
    P.TxHeartbeat.addFrom(builder, from)
    P.TxHeartbeat.addTransactionUuid(builder, encode(builder, o.transactionUUID))
    P.TxHeartbeat.endTxHeartbeat(builder)
  }
  def decode(n: P.TxHeartbeat): TxHeartbeat = {
    val to = decode(n.to())
    val from = decode(n.from())
    val transactionUUID = decode(n.transactionUuid())
    
    TxHeartbeat(to, from, transactionUUID)
  }

  def encode(builder:FlatBufferBuilder, o:TxStatusRequest): Int = {
    val to = encode(builder, o.to)
    val from = encode(builder, o.from)

    P.TxStatusRequest.startTxStatusRequest(builder)
    P.TxStatusRequest.addTo(builder, to)
    P.TxStatusRequest.addFrom(builder, from)
    P.TxStatusRequest.addTransactionUuid(builder, encode(builder, o.transactionUUID))
    P.TxStatusRequest.addRequestUuid(builder, encode(builder, o.requestUUID))
    P.TxStatusRequest.endTxStatusRequest(builder)
  }
  def decode(n: P.TxStatusRequest): TxStatusRequest = {
    val to = decode(n.to())
    val from = decode(n.from())
    val transactionUUID = decode(n.transactionUuid())
    val requestUUID = decode(n.requestUuid())

    TxStatusRequest(to, from, transactionUUID, requestUUID)
  }

  def encode(builder:FlatBufferBuilder, o:TxStatusResponse): Int = {
    val to = encode(builder, o.to)
    val from = encode(builder, o.from)

    P.TxStatusResponse.startTxStatusResponse(builder)
    P.TxStatusResponse.addTo(builder, to)
    P.TxStatusResponse.addFrom(builder, from)
    P.TxStatusResponse.addTransactionUuid(builder, encode(builder, o.transactionUUID))
    P.TxStatusResponse.addRequestUuid(builder, encode(builder, o.requestUUID))
    val haveStatus = o.status match {
      case None => false
      case Some(s) =>
        P.TxStatusResponse.addStatus(builder, encodeTransactionStatus(s.status))
        P.TxStatusResponse.addIsFinalized(builder, s.finalized)
        true
    }
    P.TxStatusResponse.addHaveStatus(builder, haveStatus)
    P.TxStatusResponse.endTxStatusResponse(builder)
  }
  def decode(n: P.TxStatusResponse): TxStatusResponse = {
    val to = decode(n.to())
    val from = decode(n.from())
    val transactionUUID = decode(n.transactionUuid())
    val requestUUID = decode(n.requestUuid())
    val ostatus = if (n.haveStatus) {
      Some(TxStatusResponse.TxStatus(decodeTransactionStatus(n.status()), n.isFinalized))
    } else None

    TxStatusResponse(to, from, transactionUUID, requestUUID, ostatus)
  }

  //-----------------------------------------------------------------------------------------------
  // Allocation Messages
  //-----------------------------------------------------------------------------------------------
  
  def encode(builder:FlatBufferBuilder, o:Allocate): Int = {
    val toStore = encode(builder, o.toStore)
    val clientData = P.Allocate.createFromClientVector(builder, o.fromClient.serialized)
    val allocObj = encode(builder, o.revisionGuard.pointer)
    builder.createUnintializedVector(1, o.objectData.size, 1).put(o.objectData.asReadOnlyBuffer())
    val objectData = builder.endVector()

    val (key, isKeyGuard) = o.revisionGuard match {
      case _: ObjectAllocationRevisionGuard => (-1, false)
      case kv: KeyValueAllocationRevisionGuard =>
        builder.createUnintializedVector(1, kv.key.bytes.length, 1).put(kv.key.bytes)
        (builder.endVector(), true)
    }
    
    P.Allocate.startAllocate(builder)
    P.Allocate.addToStore(builder, toStore)
    P.Allocate.addFromClient(builder, clientData)
    
    P.Allocate.addNewObjectUUID(builder, encode(builder, o.newObjectUUID))
    o.objectSize.foreach( sz => P.Allocate.addObjectSize(builder, sz) )
    P.Allocate.addObjectData(builder, objectData)
    P.Allocate.addInitialRefcount(builder, encode(builder, o.initialRefcount))
    P.Allocate.addObjectType(builder, o.options match {
      case _: DataAllocationOptions => P.ObjectType.Data
      case _: KeyValueAllocationOptions => P.ObjectType.KeyValue
    })
    
    P.Allocate.addTimestamp(builder, o.timestamp.asLong)
    P.Allocate.addAllocationTransactionUUID(builder, encode(builder, o.allocationTransactionUUID))
    P.Allocate.addAllocatingObject(builder, allocObj)
    P.Allocate.addAllocatingObjectRevision(builder, encodeObjectRevision(builder, o.revisionGuard.requiredRevision))
    P.Allocate.addIsKeyGuard(builder, isKeyGuard)

    if (isKeyGuard)
      P.Allocate.addAllocatingObjectKey(builder, key)

    P.Allocate.endAllocate(builder)
  }
  def decode(n: P.Allocate): Allocate = {
    val toStore = decode(n.toStore())
    val fromClient = new Array[Byte](n.fromClientLength())
    n.fromClientAsByteBuffer().get(fromClient)

    val allocationTransactionUUID = decode(n.allocationTransactionUUID())
    val allocatingObject = decode(n.allocatingObject())
    val allocatingObjectRevision = decode(n.allocatingObjectRevision())

    val revisionGuard = if (n.isKeyGuard) {
      val kbytes = new Array[Byte](n.allocatingObjectKeyLength())
      n.allocatingObjectKeyAsByteBuffer().get(kbytes)
      KeyValueAllocationRevisionGuard(allocatingObject.asInstanceOf[KeyValueObjectPointer],
        Key(kbytes), allocatingObjectRevision)
    } else {
      ObjectAllocationRevisionGuard(allocatingObject, allocatingObjectRevision)
    }

    val timestamp = HLCTimestamp(n.timestamp())
    
    val newObjectUUID = decode(n.newObjectUUID())
    val objectSize = if (n.objectSize() == 0) None else Some(n.objectSize())
    val objectData = ByteBuffer.allocateDirect(n.objectDataLength())
    objectData.put(n.objectDataAsByteBuffer())
    objectData.position(0)
    val initialRefcount = decode(n.initialRefcount())
    val options = n.objectType match {
      case P.ObjectType.Data => new DataAllocationOptions
      case P.ObjectType.KeyValue => new KeyValueAllocationOptions
    }
        
    Allocate(toStore, ClientID(fromClient), newObjectUUID, options, objectSize, initialRefcount, DataBuffer(objectData), timestamp,
        allocationTransactionUUID, revisionGuard)
  }
  
  def encode(builder:FlatBufferBuilder, o:AllocateResponse): Int = {
    val fromStoreID = encode(builder, o.fromStoreId)
    val storePointer = o.result match {
      case Right(sp) => encode(builder, sp)
      case Left(_) => -1
    }
    
    P.AllocateResponse.startAllocateResponse(builder)
    P.AllocateResponse.addFromStoreID(builder, fromStoreID)
    P.AllocateResponse.addAllocationTransactionUUID(builder, encode(builder, o.allocationTransactionUUID))
    P.AllocateResponse.addNewObjectUUID(builder, encode(builder, o.newObjectUUID))
    o.result match {
      case Right(sp) => P.AllocateResponse.addAllocatedStorePointer(builder, storePointer)
      case Left(err) => P.AllocateResponse.addResultError(builder, err match {
        case AllocationErrors.InsufficientSpace => P.AllocationError.InsufficientSpace
      })
    }
    P.AllocateResponse.endAllocateResponse(builder)
  }
  def decode(n: P.AllocateResponse): AllocateResponse = {
    val fromStoreId = decode(n.fromStoreID())
    val allocationTransactionUUID = decode(n.allocationTransactionUUID())
    val newObjectUUID = decode(n.newObjectUUID())
    val result = if (n.allocatedStorePointer() == null) {
      n.resultError() match {
        case P.AllocationError.InsufficientSpace => Left(AllocationErrors.InsufficientSpace)
      }
    } else {
      Right(decode(n.allocatedStorePointer()))
    }
    AllocateResponse(fromStoreId, allocationTransactionUUID, newObjectUUID, result)
  }

  
  //-----------------------------------------------------------------------------------------------
  // Read Messages
  //-----------------------------------------------------------------------------------------------
  
  def encodeKeyComparison(c: KeyOrdering): Byte = c match {
    case ByteArrayKeyOrdering => P.KeyComparison.ByteArray
    case IntegerKeyOrdering   => P.KeyComparison.Integer
    case LexicalKeyOrdering  => P.KeyComparison.Lexical
  }
  def decodeKeyComparison(b: Byte): KeyOrdering = b match {
    case P.KeyComparison.ByteArray => ByteArrayKeyOrdering
    case P.KeyComparison.Integer   => IntegerKeyOrdering
    case P.KeyComparison.Lexical   => LexicalKeyOrdering
  }
  
  def encode(builder:FlatBufferBuilder, o:Read): Int = {
    val toStore = encode(builder, o.toStore)
    val clientData = P.Read.createFromClientVector(builder, o.fromClient.serialized)
    val optr = encode(builder, o.objectPointer)
    val keyOffset = o.readType match {
      case rt: SingleKey          => P.Read.createKeyVector(builder, rt.key.bytes)
      case rt: LargestKeyLessThan => P.Read.createKeyVector(builder, rt.key.bytes)
      case _ => -1
    }
    val (minOffset, maxOffset) = o.readType match {
      case rt: KeyRange => 
        val min = P.Read.createMinVector(builder, rt.minimum.bytes)
        val max = P.Read.createMaxVector(builder, rt.maximum.bytes)
        (min, max)
      
      case _ => (-1, -1)
    }
    
    P.Read.startRead(builder)
    P.Read.addToStore(builder, toStore)
    P.Read.addFromClient(builder, clientData)
    P.Read.addReadUUID(builder, encode(builder, o.readUUID))
    P.Read.addObjectPointer(builder, optr)

    o.readType match {
      case rt: MetadataOnly       => P.Read.addReadType(builder, P.ReadType.MetadataOnly)
      case rt: FullObject         => P.Read.addReadType(builder, P.ReadType.FullObject)
      case rt: ByteRange          => P.Read.addReadType(builder, P.ReadType.ByteRange)
        P.Read.addOffset(builder, rt.offset)
        P.Read.addLength(builder, rt.length)
      case rt: SingleKey          => P.Read.addReadType(builder, P.ReadType.SingleKey)
        P.Read.addKey(builder, keyOffset)
        P.Read.addComparison(builder, encodeKeyComparison(rt.ordering))
      case rt: LargestKeyLessThan => P.Read.addReadType(builder, P.ReadType.LargestKeyLessThan)
        P.Read.addKey(builder, keyOffset)
        P.Read.addComparison(builder, encodeKeyComparison(rt.ordering))
      case rt: LargestKeyLessThanOrEqualTo => P.Read.addReadType(builder, P.ReadType.LargestKeyLessThan)
        P.Read.addKey(builder, keyOffset)
        P.Read.addComparison(builder, encodeKeyComparison(rt.ordering))
      case rt: KeyRange           => P.Read.addReadType(builder, P.ReadType.KeyRange)
        P.Read.addMin(builder, minOffset)
        P.Read.addMax(builder, maxOffset)
        P.Read.addComparison(builder, encodeKeyComparison(rt.ordering))
    }
    
    P.Read.endRead(builder)
  }
  def decode(n: P.Read): Read = {
    val toStore = decode(n.toStore())
    val fromClient = new Array[Byte](n.fromClientLength())
    n.fromClientAsByteBuffer().get(fromClient)
    val readUUID = decode(n.readUUID())
    val objectPointer = decode(n.objectPointer())
    
    import scala.language.implicitConversions
    
    implicit def bb2arr(bb: ByteBuffer): Array[Byte] = {
      val arr = new Array[Byte](bb.remaining())
      bb.asReadOnlyBuffer().get(arr)
      arr
    }
    
    val readType = n.readType() match {
      case P.ReadType.MetadataOnly                => MetadataOnly()           
      case P.ReadType.FullObject                  => FullObject()
      case P.ReadType.ByteRange                   => ByteRange(n.offset(), n.length())
      case P.ReadType.SingleKey                   => SingleKey(Key(n.keyAsByteBuffer()), decodeKeyComparison(n.comparison()))
      case P.ReadType.LargestKeyLessThan          => LargestKeyLessThan(Key(n.keyAsByteBuffer()), decodeKeyComparison(n.comparison()))
      case P.ReadType.LargestKeyLessThanOrEqualTo => LargestKeyLessThanOrEqualTo(Key(n.keyAsByteBuffer()), decodeKeyComparison(n.comparison()))
      case P.ReadType.KeyRange                    => KeyRange(Key(n.minAsByteBuffer()), Key(n.maxAsByteBuffer()), decodeKeyComparison(n.comparison()))
    }
    
    Read(toStore, ClientID(fromClient), readUUID, objectPointer, readType)
  }
  
  def encodeReadError(err: ObjectReadError.Value): Byte = err match {
    case ObjectReadError.ObjectMismatch => P.ObjectReadError.ObjectMismatch
    case ObjectReadError.InvalidLocalPointer => P.ObjectReadError.InvalidLocalPointer
    case ObjectReadError.CorruptedObject => P.ObjectReadError.CorruptedObject
  }
  def decodeReadError(err: Byte): ObjectReadError.Value = err match {
    case P.ObjectReadError.ObjectMismatch => ObjectReadError.ObjectMismatch
    case P.ObjectReadError.InvalidLocalPointer => ObjectReadError.InvalidLocalPointer
    case P.ObjectReadError.CorruptedObject => ObjectReadError.CorruptedObject
  }
  
  def getLockType(lock: Lock): Byte = lock match {
    case _:RevisionWriteLock => P.LockType.RevisionWriteLock
    case _:RevisionReadLock  => P.LockType.RevisionReadLock
    case _:RefcountWriteLock => P.LockType.RefcountWriteLock
    case _:RefcountReadLock  => P.LockType.RefcountReadLock
  }
  
  def encode(builder:FlatBufferBuilder, o:Lock): Int = {
    val txd = encode(builder, o.txd)
    P.Lock.startLock(builder)
    P.Lock.addType(builder, getLockType(o))
    P.Lock.addTxd(builder, txd)
    P.Lock.endLock(builder)
  }
  def decode(n: P.Lock): Lock = {
    val txd = decode(n.txd())
    n.`type`() match {
      case P.LockType.RevisionWriteLock  => RevisionWriteLock(txd)
      case P.LockType.RevisionReadLock   => RevisionReadLock(txd)
      case P.LockType.RefcountWriteLock  => RefcountWriteLock(txd)
      case P.LockType.RefcountReadLock   => RefcountReadLock(txd)
    }
  }
  
  def encode(builder:FlatBufferBuilder, o:ReadResponse): Int = {
    val fromStore = encode(builder, o.fromStore)
    
    val (objectData, locks) = o.result match {
      case Left(_) => (-1, -1)
      case Right(cs) => 
        val od = cs.objectData match {
          case None => -1
          case Some(d) =>
            builder.createUnintializedVector(1, d.size, 1).put(d.asReadOnlyBuffer())
            builder.endVector()
        }
        val locks = if (cs.lockedWriteTransactions.isEmpty) -1 else {
          val arr = new Array[Byte](16 * cs.lockedWriteTransactions.size)
          val bb = ByteBuffer.wrap(arr)
          bb.order(ByteOrder.BIG_ENDIAN)
          cs.lockedWriteTransactions.foreach { u =>
            bb.putLong(u.getMostSignificantBits)
            bb.putLong(u.getLeastSignificantBits)
          }
          P.ReadResponse.createLockedWriteTransactionsVector(builder, arr)
        }
        
        (od, locks)
    }
    
    P.ReadResponse.startReadResponse(builder)
    P.ReadResponse.addFromStore(builder, fromStore)
    P.ReadResponse.addReadUUID(builder, encode(builder, o.readUUID))
    P.ReadResponse.addReadTime(builder, o.readTime.asLong)
    o.result match {
      case Left(err) => 
        P.ReadResponse.addReadError(builder, encodeReadError(err))
        
      case Right(cs) =>
        P.ReadResponse.addRevision(builder, encodeObjectRevision(builder, cs.revision))
        P.ReadResponse.addRefcount(builder, encode(builder, cs.refcount))
        P.ReadResponse.addTimestamp(builder, cs.timestamp.asLong)
        P.ReadResponse.addSizeOnStore(builder, cs.sizeOnStore)
        P.ReadResponse.addHaveData(builder, cs.objectData.isDefined)
        if (objectData != -1) P.ReadResponse.addObjectData(builder, objectData)
        if (locks != -1) P.ReadResponse.addLockedWriteTransactions(builder, locks)
    }
    P.ReadResponse.endReadResponse(builder)
  }
  def decode(n: P.ReadResponse): ReadResponse = {
    val fromStore = decode(n.fromStore())
    val readUUID = decode(n.readUUID())
    val readTime = HLCTimestamp(n.readTime())
    val result = if (n.revision() == null) {
      Left(decodeReadError(n.readError()))
    } else {
      val revision = decode(n.revision())
      val refcount = decode(n.refcount())
      val timestamp = HLCTimestamp(n.timestamp())
      val sizeOnStore = n.sizeOnStore()
      val objectData = if (!n.haveData()) None else {
        val buff = ByteBuffer.allocateDirect(n.objectDataLength())
        if (n.objectDataLength() > 0)
          buff.put(n.objectDataAsByteBuffer())
        buff.position(0)
        Some(buff)
      }
      
      var lockedWriteTransactions = Set[UUID]()
      if (n.lockedWriteTransactionsLength() > 0) {
        val lbb = n.lockedWriteTransactionsAsByteBuffer()
        lbb.order(ByteOrder.BIG_ENDIAN)
        while (lbb.remaining() != 0) {
          val msb = lbb.getLong()
          val lsb = lbb.getLong()
          lockedWriteTransactions += new UUID(msb, lsb) 
        }
      }
      
      Right(ReadResponse.CurrentState(revision, refcount, timestamp, sizeOnStore, objectData.map(DataBuffer(_)), lockedWriteTransactions))
    }
    ReadResponse(fromStore, readUUID, readTime, result)
  }
  
  
  def encode(builder:FlatBufferBuilder, o:OpportunisticRebuild): Int = {
    val toStore = encode(builder, o.toStore)
    val clientData = P.OpportunisticRebuild.createFromClientVector(builder, o.fromClient.serialized)
    val optr = encode(builder, o.pointer)
    builder.createUnintializedVector(1, o.data.size, 1).put(o.data.asReadOnlyBuffer())
    val data = builder.endVector()

    P.OpportunisticRebuild.startOpportunisticRebuild(builder)
    P.OpportunisticRebuild.addToStore(builder, toStore)
    P.OpportunisticRebuild.addFromClient(builder, clientData)
    P.OpportunisticRebuild.addPointer(builder, optr)
    P.OpportunisticRebuild.addRevision(builder, encodeObjectRevision(builder, o.revision))
    P.OpportunisticRebuild.addRefcount(builder, encode(builder, o.refcount))
    P.OpportunisticRebuild.addTimestamp(builder, o.timestamp.asLong)
    P.OpportunisticRebuild.addData(builder, data)
    P.OpportunisticRebuild.endOpportunisticRebuild(builder)
  }
  
  def decode(n: P.OpportunisticRebuild): OpportunisticRebuild = {
    val toStore = decode(n.toStore())
    val fromClient = new Array[Byte](n.fromClientLength())
    n.fromClientAsByteBuffer().get(fromClient)
    val pointer = decode(n.pointer())
    val revision = decode(n.revision())
    val refcount = decode(n.refcount())
    val timestamp = HLCTimestamp(n.timestamp())
    
    val buff = ByteBuffer.allocateDirect(n.dataLength())
    buff.put(n.dataAsByteBuffer())
    buff.position(0)
    
    val data = DataBuffer(buff)
    
    OpportunisticRebuild(toStore, ClientID(fromClient), pointer, revision, refcount, timestamp, data)
  }

  def encode(builder:FlatBufferBuilder, o:TransactionCompletionQuery): Int = {
    val toStore = encode(builder, o.toStore)
    val clientData = P.TransactionCompletionQuery.createFromClientVector(builder, o.fromClient.serialized)

    P.TransactionCompletionQuery.startTransactionCompletionQuery(builder)
    P.TransactionCompletionQuery.addToStore(builder, toStore)
    P.TransactionCompletionQuery.addFromClient(builder, clientData)
    P.TransactionCompletionQuery.addQueryUUID(builder, encode(builder, o.queryUUID))
    P.TransactionCompletionQuery.addTransactionUUID(builder, encode(builder, o.transactionUUID))
    P.TransactionCompletionQuery.endTransactionCompletionQuery(builder)
  }

  def decode(n: P.TransactionCompletionQuery): TransactionCompletionQuery = {
    val toStore = decode(n.toStore())
    val fromClient = new Array[Byte](n.fromClientLength())
    n.fromClientAsByteBuffer().get(fromClient)
    val queryUUID = decode(n.queryUUID())
    val transactionUUID = decode(n.transactionUUID())

    TransactionCompletionQuery(toStore, ClientID(fromClient), queryUUID, transactionUUID)
  }

  def encode(builder:FlatBufferBuilder, o:TransactionCompletionResponse): Int = {
    val fromStore = encode(builder, o.fromStore)

    P.TransactionCompletionResponse.startTransactionCompletionResponse(builder)
    P.TransactionCompletionResponse.addFromStore(builder, fromStore)
    P.TransactionCompletionResponse.addQueryUUID(builder, encode(builder, o.queryUUID))
    P.TransactionCompletionResponse.addIsComplete(builder, o.isComplete)
    P.TransactionCompletionResponse.endTransactionCompletionResponse(builder)
  }
  
  def decode(n: P.TransactionCompletionResponse): TransactionCompletionResponse = {
    val fromStore = decode(n.fromStore())
    val queryUUID = decode(n.queryUUID())
    val isComplete = n.isComplete

    TransactionCompletionResponse(fromStore, queryUUID, isComplete)
  }
}