package com.ibm.aspen.base.impl

import java.util.UUID
import com.ibm.aspen.core.objects.ObjectRevision
import com.ibm.aspen.core.objects.ObjectRefcount
import java.nio.ByteBuffer
import com.ibm.aspen.core.objects.ObjectPointer
import com.ibm.aspen.core.ida.Replication
import com.ibm.aspen.core.objects.StorePointer
import com.ibm.aspen.core.read.ReadDriver
import com.ibm.aspen.core.read.ReadResponse
import scala.concurrent.Future
import com.ibm.aspen.core.read.ReadError
import com.ibm.aspen.core.network.ClientSideReadMessenger
import com.ibm.aspen.core.read.InvalidObject
import scala.concurrent.Promise
import com.ibm.aspen.core.allocation.AllocationErrors
import com.ibm.aspen.core.data_store.DataStoreID
import com.ibm.aspen.core.network.ClientSideAllocationMessenger
import com.ibm.aspen.core.ida.IDA
import com.ibm.aspen.core.allocation.AllocationDriver
import com.ibm.aspen.core.transaction.ClientTransactionDriver
import com.ibm.aspen.base.kvtree.KVTreeNodeCache
import com.ibm.aspen.core.network.StorageNodeID
import com.ibm.aspen.core.network.ClientSideTransactionMessageReceiver
import com.ibm.aspen.core.network.ClientSideReadMessageReceiver
import com.ibm.aspen.core.network.ClientSideAllocationMessageReceiver
import com.ibm.aspen.core.network.ClientSideTransactionMessenger
import com.ibm.aspen.core.network.ClientID
import com.ibm.aspen.core.allocation
import com.ibm.aspen.core.read
import com.ibm.aspen.core.transaction.TxPrepare
import com.ibm.aspen.base.kvtree.KVTree
import scala.concurrent.ExecutionContext.Implicits.global
import com.ibm.aspen.base.kvtree.KVTreeSimpleFactory
import com.ibm.aspen.base.AspenSystem
import com.ibm.aspen.base.NoRetry
import com.ibm.aspen.core.objects.ObjectState
import com.ibm.aspen.base.Transaction
import scala.Left
import scala.Right
import com.ibm.aspen.base.kvlist.KVList
import com.ibm.aspen.util.uuid2byte
import com.ibm.aspen.core.network.NetworkCodec
import scala.concurrent.duration._
import com.ibm.aspen.core.transaction.ClientTransactionManager
import com.ibm.aspen.core.transaction.LocalUpdate
import com.ibm.aspen.core.DataBuffer
import com.ibm.aspen.core.allocation.AllocateResponse
import com.ibm.aspen.core.network.ClientSideNetwork
import com.ibm.aspen.core.network.ClientSideTransactionHandler
import com.ibm.aspen.core.network.ClientSideReadHandler
import com.ibm.aspen.core.network.ClientSideAllocationHandler
import com.ibm.aspen.core.HLCTimestamp
import com.ibm.aspen.core.objects.DataObjectPointer
import com.ibm.aspen.core.objects.DataObjectState
import com.ibm.aspen.core.allocation.AllocationOptions
import com.ibm.aspen.core.transaction.TransactionDescription
import scala.concurrent.ExecutionContext
import com.ibm.aspen.core.data_store.Lock
import scala.concurrent.Await
import com.ibm.aspen.core.read.ReadType
import com.ibm.aspen.core.objects.keyvalue.KeyValueOperation
import com.ibm.aspen.core.transaction.KeyValueUpdate
import com.ibm.aspen.core.objects.KeyValueObjectPointer

object MockSystem {
  val poolUUID = new UUID(0,0)
  val clientID = ClientID(poolUUID)
  
  class RevisionMismatch extends Throwable
  class RefcountMismatch extends Throwable
  
  sealed abstract class ObjectOp
  case class Append(ptr: ObjectPointer, requiredRevision: ObjectRevision, buf: DataBuffer) extends ObjectOp
  case class Overwrite(ptr: ObjectPointer, requiredRevision: ObjectRevision, buf: DataBuffer) extends ObjectOp
  case class SetRef(ptr: ObjectPointer, requiredRefcount: ObjectRefcount, newRefcount: ObjectRefcount) extends ObjectOp
  
  class StorageSystem {
    private[this] var objects = Map[UUID, DataObjectState]()
    
    def read(ptr: ObjectPointer): Option[DataObjectState] = synchronized { objects.get(ptr.uuid) }
    
    def allocate(newObjectUUID: UUID, allocTxUUID: UUID, initialRefcount: ObjectRefcount, initialContent: DataBuffer, timestamp: HLCTimestamp): ObjectState = synchronized {
      val cpy = ByteBuffer.allocate(initialContent.size)
      cpy.put(initialContent.asReadOnlyBuffer())
      cpy.position(0)
      val rev = ObjectRevision(allocTxUUID)
      val ptr = DataObjectPointer(newObjectUUID, poolUUID, None, Replication(3,2), new Array[StorePointer](0))
      val osd = DataObjectState(ptr, rev, initialRefcount, timestamp, DataBuffer(cpy))
      objects += (osd.pointer.uuid -> osd)
      osd
    }
    
    def txUpdate( txUUID: UUID, ops: List[ObjectOp], timestamp: HLCTimestamp ): Unit = synchronized {
      ops.foreach( op => op match {
        case a:Append => if (a.requiredRevision != objects(a.ptr.uuid).revision) throw new RevisionMismatch
        case o:Overwrite => if (o.requiredRevision != objects(o.ptr.uuid).revision) throw new RevisionMismatch
        case r:SetRef => if (r.requiredRefcount != objects(r.ptr.uuid).refcount) throw new RefcountMismatch
      })
      ops.foreach( op => op match {
        case a:Append => append(txUUID, a.ptr, a.buf, timestamp)
        case o:Overwrite => overwrite(txUUID, o.ptr, o.buf, timestamp)
        case r:SetRef => setref(r.ptr, r.newRefcount)
      })
    }
    
    private def append(txUUID: UUID, ptr: ObjectPointer, buf: DataBuffer, timestamp: HLCTimestamp): Unit = {
      val orig = objects(ptr.uuid)
      
      val newSize = orig.data.size + buf.size
      val newData = ByteBuffer.allocate(newSize)
      newData.put(orig.data.asReadOnlyBuffer())
      newData.put(buf.asReadOnlyBuffer())
      newData.position(0)
      val newRev = ObjectRevision(txUUID)
      
      objects += (ptr.uuid -> DataObjectState(ptr, newRev, orig.refcount, timestamp, DataBuffer(newData)))
    }
    
    def overwrite(txUUID: UUID, ptr: ObjectPointer, buf: DataBuffer, timestamp: HLCTimestamp): Unit =  {
      val orig = objects(ptr.uuid)
        
      val newData = ByteBuffer.allocate(buf.size)
      newData.put(buf.asReadOnlyBuffer())
      newData.position(0)
      val newRev = ObjectRevision(txUUID)
      
      objects += (ptr.uuid -> DataObjectState(ptr, newRev, orig.refcount, timestamp, DataBuffer(newData)))
    }
    
    private def setref(ptr:ObjectPointer, newRef: ObjectRefcount): Unit = {
      val orig = objects(ptr.uuid)
      objects += (ptr.uuid -> DataObjectState(ptr, orig.revision, newRef, orig.timestamp, orig.data))
    }
  }
  
  class Reader(storage: StorageSystem, pointer:ObjectPointer) extends ReadDriver {
    
    def readResult: Future[Either[ReadError, (ObjectState, Option[Map[DataStoreID, List[Lock]]])]] = {
      val e: Either[ReadError, (ObjectState, Option[Map[DataStoreID, List[Lock]]])] = storage.read(pointer) match {
          case None => Left(new InvalidObject)
          case Some(osd) => Right((DataObjectState(pointer, osd.revision, osd.refcount, osd.timestamp, osd.data), None))
        }
      Future.successful(e)
    } 
    
    override def begin(): Unit = ()
  
    override def receiveReadResponse(response:ReadResponse): Unit = ()
  }
  class ReaderFactory(storage: StorageSystem) {
    def apply( clientMessenger: ClientSideReadMessenger,
               objectPointer: ObjectPointer,
               readType: ReadType,
               retrieveLockedTransaction: Boolean,
               readUUID:UUID) : ReadDriver = new Reader(storage, objectPointer)
  }
  
  class Tx(
      val txNumber: Int, 
      storage: StorageSystem, 
      runFinalizationActions: (Map[UUID, Array[Byte]]) => Unit) extends Transaction {
    
    val uuid = new UUID(0,txNumber)
    
    val p = Promise[Unit]()
    
    val result = p.future
    
    private var ops = List[ ObjectOp ]()
    
    private var finalizationActions = Map[UUID, Array[Byte]]()
    
    private var invalidatedReason: Option[Throwable] = None
    
    private var notifyOnResolution = Set[DataStoreID]()
    
    override def append(objectPointer: DataObjectPointer, requiredRevision: ObjectRevision, data: DataBuffer): ObjectRevision = synchronized {
      ops = Append(objectPointer, requiredRevision, data) :: ops
      txRevision
    }
    
    override def overwrite(objectPointer: DataObjectPointer, requiredRevision: ObjectRevision, data: DataBuffer): ObjectRevision =  synchronized {
      ops = Overwrite(objectPointer, requiredRevision, data) :: ops
      txRevision
    }
    
    def append(
      pointer: KeyValueObjectPointer, 
      requiredRevision: Option[ObjectRevision],
      requirements: List[KeyValueUpdate.KVRequirement],
      operations: List[KeyValueOperation]): Unit = ()
      
    def overwrite(
      pointer: KeyValueObjectPointer, 
      requiredRevision: ObjectRevision,
      requirements: List[KeyValueUpdate.KVRequirement],
      operations: List[KeyValueOperation]): Unit = ()
    
    def bumpVersion(objectPointer: ObjectPointer, requiredRevision: ObjectRevision): ObjectRevision = throw new Exception("Should not be used")
    
    override def setRefcount(objectPointer: ObjectPointer, requiredRefcount: ObjectRefcount, refcount: ObjectRefcount): ObjectRefcount = synchronized {
      ops = SetRef(objectPointer, requiredRefcount, refcount) :: ops
      refcount
    }
    
    def ensureHappensAfter(timestamp: HLCTimestamp): Unit = ()
    
    def timestamp(): HLCTimestamp = HLCTimestamp.now
    
    def invalidateTransaction(reason: Throwable): Unit = synchronized { invalidatedReason = Some(reason) }
    
    def addFinalizationAction(finalizationActionUUID: UUID, serializedContent: Array[Byte]): Unit = synchronized {
      finalizationActions += (finalizationActionUUID -> serializedContent)
    }
    
    def addNotifyOnResolution(storesToNotify: Set[DataStoreID]): Unit = notifyOnResolution ++ storesToNotify
    
    def commit()(implicit ec: ExecutionContext): Future[Unit] = synchronized {
      if (!p.isCompleted) {
        invalidatedReason match {
          case None =>
            try {
              storage.txUpdate(uuid, ops, HLCTimestamp.now)
              runFinalizationActions(finalizationActions)
              p.success(())
            } catch {
              case e: Throwable => p.failure(e)
            }
          case Some(reason) => p.failure(reason)
        }
      }
      p.future
    }
  }
 
  class AllocDriver(
      val store: StorageSystem,
      val txUUID: UUID,
      val newObjectUUID: UUID,
      val objectData: Map[Byte,DataBuffer],
      val timestamp: HLCTimestamp,
      val initialRefcount: ObjectRefcount) extends AllocationDriver {
    
    val objectPointer = store.allocate(newObjectUUID, txUUID, initialRefcount, objectData.head._2, timestamp).pointer
    
    def futureResult: Future[Either[Map[Byte,AllocationErrors.Value], ObjectPointer]] = Future.successful(Right(objectPointer))
    
    /** Initiates the allocation process */
    def start(): Unit = ()
    
    def receiveAllocationResult(fromStoreId: DataStoreID, 
                                allocationTransactionUUID: UUID, 
                                result: Either[AllocationErrors.Value, List[AllocateResponse.Allocated]]): Unit  = ()
  }
  
  class AllocDriverFactory(val store: StorageSystem) extends AllocationDriver.Factory {
    
    def create(messenger: ClientSideAllocationMessenger,
               poolUUID: UUID,
               newObjectUUID: UUID,
               objectSize: Option[Int],
               objectIDA: IDA,
               objectData: Map[Byte,DataBuffer], // Map DataStore pool index -> store-specific ObjectData
               options: AllocationOptions,
               timestamp: HLCTimestamp,
               initialRefcount: ObjectRefcount,
               allocationTransactionUUID: UUID,
               allocatingObject: ObjectPointer,
               allocatingObjectRevision: ObjectRevision): AllocationDriver = new AllocDriver(store, 
                   allocationTransactionUUID, newObjectUUID, objectData, timestamp, initialRefcount)
  
  }
  
  object cliMessenger extends ClientSideNetwork with ClientSideAllocationHandler with ClientSideReadHandler with ClientSideTransactionHandler {
    override val clientId: ClientID = clientID
    
    // ClientSideAllocationMessenger
    override def send(toStore: DataStoreID, message: allocation.Allocate): Unit = ()
    
    // ClientSideReadMessenger
    override def send(toStore: DataStoreID, message: read.Read): Unit = ()
    
    // ClientSideTransactionMessenger
    override def send(message: TxPrepare, updateContent: List[LocalUpdate]): Unit = ()

    def setReceiver(receiver: ClientSideTransactionMessageReceiver): Unit = ()
    def setReceiver(receiver: ClientSideReadMessageReceiver): Unit = ()
    def setReceiver(receiver: ClientSideAllocationMessageReceiver): Unit = ()

    val allocationHandler: ClientSideAllocationHandler = this

    val readHandler: ClientSideReadHandler = this

    val transactionHandler: ClientSideTransactionHandler = this
    
    
    
  }
}
class MockSystem(val treeNodeSize:Int=1000) {
  import MockSystem._
  
  val storage = new StorageSystem
  val bootstrapPoolIDA = new Replication(3,2)
  val noRetry = new NoRetry
  
  val clientTransactionDriverFactory = ClientTransactionDriver.noErrorRecoveryFactory _
  val storagePoolFactory = BaseStoragePool.Factory
  
  def systemTreeNodeCacheFactory(sys: AspenSystem): KVTreeNodeCache = new KVTreeNodeCache {}
  def chooseDesignatedLeader(p: ObjectPointer): Byte = 0 
  def isStorageNodeOnline(s: StorageNodeID): Boolean = true
  
  // Bootstrap the system by creating the StoragePoolDefinition for Bootstrapping Pool & StoragePoolTreeDefinition
  
  def allocate(content: DataBuffer, timestamp:HLCTimestamp): Future[DataObjectPointer] = {
    Future.successful(storage.allocate(UUID.randomUUID(), new UUID(0,0), ObjectRefcount(0,1), content, timestamp).pointer.asInstanceOf[DataObjectPointer])
  }
  
  def overwrite(ptr: ObjectPointer, arr: Array[Byte], timestamp: HLCTimestamp): Future[Unit] = {
    Future.successful(storage.overwrite(new UUID(0,0), ptr, DataBuffer(arr), timestamp))
  }
  
  var txNumber = 0
  
  def newTx(txMgr: ClientTransactionManager, fn: (ObjectPointer) => Byte, o: Option[ClientTransactionDriver.Factory]) = synchronized {
    val txn = txNumber
    txNumber += 1
    new Tx(txn, storage, runTransactionFinalizations)
  }
  
  import scala.language.postfixOps
  
  val radiclePointer = Await.result(Bootstrap.initializeNewSystem(allocate, overwrite, bootstrapPoolIDA), 900 milliseconds)
  
  val basicAspenSystem = new BasicAspenSystem( 
      chooseDesignatedLeader _,
      isStorageNodeOnline _,
      cliMessenger,
      new ReaderFactory(storage).apply _,
      clientTransactionDriverFactory,
      new AllocDriverFactory(storage),
      newTx _,
      BaseStoragePool.Factory,
      bootstrapPoolIDA,
      systemTreeNodeCacheFactory _,
      radiclePointer,
      noRetry)
  
  val kvTreeFactory = new KVTreeSimpleFactory(basicAspenSystem, poolUUID, poolUUID, Replication(3,2), treeNodeSize, new KVTreeNodeCache {}, KVTree.KeyComparison.Raw)
  val finalizationHandler = BaseFinalizationActionHandlerRegistry(noRetry, basicAspenSystem, kvTreeFactory)
  
  var finalizationActions: List[Future[Unit]] = Nil
  
  def runTransactionFinalizations(m: Map[UUID, Array[Byte]]): Unit = synchronized {
    m.foreach { t =>
      finalizationHandler.getFinalizationActionHandler(t._1) match {
        case Some(fah) =>
          val fa = fah.createAction(t._2)
          finalizationActions = fa.execute() :: finalizationActions
          
        case None => throw new Exception("Unsupported Finalization Action Encountered!")
      }
    }
  }
  
  def allFinalizationActionsCompleted = synchronized { Future.sequence(finalizationActions) } map {_ => ()}
}