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
import com.ibm.aspen.core.read.ObjectState
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
import com.ibm.aspen.base.ObjectStateAndData
import com.ibm.aspen.base.Transaction
import scala.Left
import scala.Right
import com.ibm.aspen.base.kvlist.KVList
import com.ibm.aspen.core.Util._
import com.ibm.aspen.core.network.NetworkCodec
import scala.concurrent._
import scala.concurrent.duration._
import com.ibm.aspen.core.transaction.ClientTransactionManager
import com.ibm.aspen.core.transaction.LocalUpdate

object MockSystem {
  val poolUUID = new UUID(0,0)
  val clientID = ClientID(poolUUID)
  
  class RevisionMismatch extends Throwable
  class RefcountMismatch extends Throwable
  
  sealed abstract class ObjectOp
  case class Append(ptr: ObjectPointer, requiredRevision: ObjectRevision, buf: ByteBuffer) extends ObjectOp
  case class Overwrite(ptr: ObjectPointer, requiredRevision: ObjectRevision, buf: ByteBuffer) extends ObjectOp
  case class SetRef(ptr: ObjectPointer, requiredRefcount: ObjectRefcount, newRefcount: ObjectRefcount) extends ObjectOp
  
  class StorageSystem {
    private[this] var objects = Map[UUID, ObjectStateAndData]()
    
    def read(ptr: ObjectPointer): Option[ObjectStateAndData] = synchronized { objects.get(ptr.uuid) }
    
    def allocate(newObjectUUID: UUID, initialRefcount: ObjectRefcount, initialContent: ByteBuffer): ObjectStateAndData = synchronized {
      val len = initialContent.limit() - initialContent.position()
      val cpy = ByteBuffer.allocate(len)
      cpy.put(initialContent.asReadOnlyBuffer())
      cpy.position(0)
      val rev = ObjectRevision(0, len)
      val ptr = ObjectPointer(newObjectUUID, poolUUID, None, Replication(3,2), new Array[StorePointer](0))
      val osd = ObjectStateAndData(ptr, rev, initialRefcount, cpy.asReadOnlyBuffer())
      objects += (osd.pointer.uuid -> osd)
      osd
    }
    
    def txUpdate( ops: List[ObjectOp] ): Unit = synchronized {
      ops.foreach( op => op match {
        case a:Append => if (a.requiredRevision != objects(a.ptr.uuid).revision) throw new RevisionMismatch
        case o:Overwrite => if (o.requiredRevision != objects(o.ptr.uuid).revision) throw new RevisionMismatch
        case r:SetRef => if (r.requiredRefcount != objects(r.ptr.uuid).refcount) throw new RefcountMismatch
      })
      ops.foreach( op => op match {
        case a:Append => append(a.ptr, a.buf)
        case o:Overwrite => overwrite(o.ptr, o.buf)
        case r:SetRef => setref(r.ptr, r.newRefcount)
      })
    }
    
    private def append(ptr: ObjectPointer, buf: ByteBuffer): Unit = {
      val orig = objects(ptr.uuid)
      
      val bufLen = buf.limit() - buf.position()
      val newSize = orig.data.capacity() + bufLen
      val newData = ByteBuffer.allocate(newSize)
      newData.put(orig.data.asReadOnlyBuffer())
      newData.put(buf.asReadOnlyBuffer())
      newData.position(0)
      val newRev = orig.revision.copy(currentSize = newSize)
      
      objects += (ptr.uuid -> ObjectStateAndData(ptr, newRev, orig.refcount, newData.asReadOnlyBuffer()))
    }
    
    def overwrite(ptr: ObjectPointer, buf: ByteBuffer): Unit =  {
      val orig = objects(ptr.uuid)
        
      val bufLen = buf.limit() - buf.position()
      val newData = ByteBuffer.allocate(bufLen)
      newData.put(buf.asReadOnlyBuffer())
      newData.position(0)
      val newRev = orig.revision.copy(overwriteCount=orig.revision.overwriteCount+1, currentSize=bufLen)
      
      objects += (ptr.uuid -> ObjectStateAndData(ptr, newRev, orig.refcount, newData.asReadOnlyBuffer()))
    }
    
    private def setref(ptr:ObjectPointer, newRef: ObjectRefcount): Unit = {
      val orig = objects(ptr.uuid)
      objects += (ptr.uuid -> ObjectStateAndData(ptr, orig.revision, newRef, orig.data))
    }
  }
  
  class Reader(storage: StorageSystem, pointer:ObjectPointer) extends ReadDriver {
    
    def readResult: Future[Either[ReadError, ObjectState]] = {
      val e: Either[ReadError, ObjectState] = storage.read(pointer) match {
          case None => Left(new InvalidObject)
          case Some(osd) => Right(ObjectState(pointer, osd.revision, osd.refcount, Some(osd.data), None))
        }
      Future.successful(e)
    } 
  
    override def receiveReadResponse(response:ReadResponse): Unit = ()
  }
  class ReaderFactory(storage: StorageSystem) {
    def apply( clientMessenger: ClientSideReadMessenger,
               objectPointer: ObjectPointer,
               retrieveObjectData: Boolean,
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
    
    override def append(objectPointer: ObjectPointer, requiredRevision: ObjectRevision, data: ByteBuffer): ObjectRevision = synchronized {
      val len = data.limit() - data.position()
      ops = Append(objectPointer, requiredRevision, data) :: ops
      requiredRevision.append(len)
    }
    
    override def overwrite(objectPointer: ObjectPointer, requiredRevision: ObjectRevision, data: ByteBuffer): ObjectRevision =  synchronized {
      val len = data.limit - data.position
      ops = Overwrite(objectPointer, requiredRevision, data) :: ops
      requiredRevision.overwrite(len)
    }
    
    override def setRefcount(objectPointer: ObjectPointer, requiredRefcount: ObjectRefcount, refcount: ObjectRefcount): ObjectRefcount = synchronized {
      ops = SetRef(objectPointer, requiredRefcount, refcount) :: ops
      refcount
    }
    
    def invalidateTransaction(reason: Throwable): Unit = synchronized { invalidatedReason = Some(reason) }
    
    def addFinalizationAction(finalizationActionUUID: UUID, serializedContent: Array[Byte]): Unit = synchronized {
      finalizationActions += (finalizationActionUUID -> serializedContent)
    }
    
    def commit()(implicit ec: ExecutionContext): Future[Unit] = synchronized {
      if (!p.isCompleted) {
        invalidatedReason match {
          case None =>
            try {
              storage.txUpdate(ops)
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
      val newObjectUUID: UUID,
      val objectData: Map[Byte,ByteBuffer],
      val initialRefcount: ObjectRefcount) extends AllocationDriver {
    
    val objectPointer = store.allocate(newObjectUUID, initialRefcount, objectData.head._2).pointer
    
    def futureResult: Future[Either[Map[Byte,AllocationErrors.Value], ObjectPointer]] = Future.successful(Right(objectPointer))
    
    /** Initiates the allocation process */
    def start(): Unit = ()
    
    def receiveAllocationResult(fromStoreId: DataStoreID, 
                                allocationTransactionUUID: UUID, 
                                result: Either[AllocationErrors.Value, StorePointer]): Unit  = ()
  }
  
  class AllocDriverFactory(val store: StorageSystem) extends AllocationDriver.Factory {
    
    def create(messenger: ClientSideAllocationMessenger,
               poolUUID: UUID,
               newObjectUUID: UUID,
               objectSize: Option[Int],
               objectIDA: IDA,
               objectData: Map[Byte,ByteBuffer], // Map DataStore pool index -> store-specific ObjectData
               initialRefcount: ObjectRefcount,
               allocationTransactionUUID: UUID,
               allocatingObject: ObjectPointer,
               allocatingObjectRevision: ObjectRevision): AllocationDriver = new AllocDriver(store, newObjectUUID, objectData, initialRefcount)
  
  }
  
  object cliMessenger extends ClientMessenger with ClientSideAllocationMessenger with ClientSideReadMessenger with ClientSideTransactionMessenger{
    val clientId: ClientID = clientID
    
    // ClientSideAllocationMessenger
    override def send(toStore: DataStoreID, message: allocation.Allocate): Unit = ()
    
    // ClientSideReadMessenger
    override def send(toStore: DataStoreID, message: read.Read): Unit = ()
    
    // ClientSideTransactionMessenger
    override def send(message: TxPrepare, updateContent: List[LocalUpdate]): Unit = ()
    
    override def setMessageReceivers(
      transactionMessageReceiver: ClientSideTransactionMessageReceiver,
      readMessageReceiver: ClientSideReadMessageReceiver,
      allocationMessageReceiver: ClientSideAllocationMessageReceiver): Unit = ()
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
  
  def allocate(content: ByteBuffer): Future[ObjectPointer] = {
    Future.successful(storage.allocate(UUID.randomUUID(), ObjectRefcount(0,1), content).pointer)
  }
  
  def overwrite(ptr: ObjectPointer, arr: Array[Byte]): Future[Unit] = {
    Future.successful(storage.overwrite(ptr, ByteBuffer.wrap(arr)))
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
  val finalizationHandler = FinalizationActionRegistry.initialize(noRetry, basicAspenSystem, kvTreeFactory)
  
  var finalizationActions: List[Future[Unit]] = Nil
  
  def runTransactionFinalizations(m: Map[UUID, Array[Byte]]): Unit = synchronized {
    m.foreach { t =>
      finalizationHandler.createAction(t._1, t._2) match {
        case Some(fa) => finalizationActions = fa.execute() :: finalizationActions
        case None => throw new Exception("Unsupported Finalization Action Encountered!")
      }
    }
  }
  
  def allFinalizationActionsCompleted = synchronized { Future.sequence(finalizationActions) } map {_ => ()}
}