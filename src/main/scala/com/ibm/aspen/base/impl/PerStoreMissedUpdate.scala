package com.ibm.aspen.base.impl

import java.nio.ByteBuffer
import java.util.UUID

import com.ibm.aspen.base._
import com.ibm.aspen.base.tieredlist.{MutableKeyValueObjectRootManager, MutableTieredKeyValueList, SimpleTieredKeyValueListNodeAllocater, TieredKeyValueListRoot}
import com.ibm.aspen.core.data_store.DataStoreID
import com.ibm.aspen.core.objects.keyvalue.{ByteArrayKeyOrdering, Insert, Key}
import com.ibm.aspen.core.objects.{KeyValueObjectState, ObjectPointer}
import com.ibm.aspen.core.read.CorruptedObject
import com.ibm.aspen.core.transaction.KeyValueUpdate
import com.ibm.aspen.util.byte2uuid

import scala.concurrent.{ExecutionContext, Future}

/**
 *  Strategy: Store in the pool definition a MutableTieredKeyValueList for each store that contains the UUIDs of
 *  all objects peer nodes thing the store may have missed. Concurrent modification performance should be decent
 *  due to blind appends and multiple updates to the same object will be compacted down to a single entry during
 *  pre-split compaction.
 *  
 */
object PerStoreMissedUpdate extends MissedUpdateHandlerFactory {
 
  val typeUUID: UUID = UUID.fromString("fed25913-19e0-4045-b45c-2fc30d3200f1")
  
  def getStrategy(objectAllocaters: Array[UUID], tierNodeSizes: Array[Int], tierKVPairLimits: Array[Int]): MissedUpdateStrategy = {
    MissedUpdateStrategy(typeUUID, Some(encodeTreeConfig(objectAllocaters, tierNodeSizes, tierKVPairLimits)))
  }
  
  def encodeTreeConfig(objectAllocaters: Array[UUID], tierNodeSizes: Array[Int], tierNodeKVPairLimits: Array[Int]): Array[Byte] = {
    val arr = new Array[Byte](1 + 1 + 1+ 16 * objectAllocaters.length + 4 * tierNodeSizes.length + 4 * tierNodeKVPairLimits.length)
    val bb = ByteBuffer.wrap(arr)
    bb.put(objectAllocaters.length.asInstanceOf[Byte])
    bb.put(tierNodeSizes.length.asInstanceOf[Byte])
    bb.put(tierNodeKVPairLimits.length.asInstanceOf[Byte])
    objectAllocaters.foreach { u =>
      bb.putLong(u.getMostSignificantBits)
      bb.putLong(u.getLeastSignificantBits)
    }
    tierNodeSizes.foreach( i => bb.putInt(i) )
    tierNodeKVPairLimits.foreach( i => bb.putInt(i) )
    arr
  }
  
  def decodeTreeConfig(arr: Array[Byte]): (Array[UUID], Array[Int], Array[Int]) = {
    val bb = ByteBuffer.wrap(arr)
    val nu = bb.get()
    val ni = bb.get()
    val nl = bb.get()
    val objectAllocaters = (0 until nu).map { _ =>
      val msb = bb.getLong()
      val lsb = bb.getLong()
      new UUID(msb, lsb)
    }.toArray
    val tierNodeSizes = (0 until ni).map( _ => bb.getInt() ).toArray
    val tierNodeKVPairLimits = (0 until nl).map( _ => bb.getInt() ).toArray
    (objectAllocaters, tierNodeSizes, tierNodeKVPairLimits)
  }
  
  def storeKey(storeIndex: Byte): Key = {
    // prefix with UUID to prevent key clashes
    val arr = new Array[Byte](17)
    val bb = ByteBuffer.wrap(arr)
    bb.putLong(typeUUID.getMostSignificantBits)
    bb.putLong(typeUUID.getLeastSignificantBits)
    bb.put(storeIndex)
    Key(arr)
  }
  
  def loadMissedUpdateTree(
      system: AspenSystem, 
      poolUUID: UUID, 
      storeIndex: Byte)(implicit ec: ExecutionContext): Future[MutableTieredKeyValueList] = {
    
    val treeKey = storeKey(storeIndex)
    
    // Fail if the pool object has been deleted
    def onAttemptFailure(t: Throwable): Future[Unit] = t match {
      case t: CorruptedObject => throw StopRetrying(t)
      case _ => Future.unit
    }

    def getOrCreate(pool: StoragePool, kvos: KeyValueObjectState): Future[MutableTieredKeyValueList] = {

      def attempt(): Future[TieredKeyValueListRoot] = {
        kvos.contents.get(treeKey) match {
          case Some(v) => Future.successful(TieredKeyValueListRoot(v.value))

          case None =>
            val (objectAllocaters, tierNodeSizes, tierKVPairLimits) = decodeTreeConfig(pool.getMissedUpdateStrategy().config.get)

            val allocaterType = SimpleTieredKeyValueListNodeAllocater.typeUUID
            val allocaterConfig = SimpleTieredKeyValueListNodeAllocater.encode(objectAllocaters, tierNodeSizes, tierKVPairLimits)

            system.transact { implicit tx =>
              for {
                allocater <- system.getObjectAllocater(objectAllocaters.head)

                rootNode <- allocater.allocateKeyValueObject(kvos.pointer, kvos.revision, Nil)

                root = TieredKeyValueListRoot(0, ByteArrayKeyOrdering, rootNode, allocaterType, allocaterConfig)

                reqs = KeyValueUpdate.KVRequirement(treeKey, kvos.timestamp, KeyValueUpdate.TimestampRequirement.DoesNotExist) :: Nil

                _=tx.note(s"Creating MissedUpdateTree ${rootNode.uuid} for pool $poolUUID, store $storeIndex")

                _=tx.update(kvos.pointer, Some(kvos.revision), reqs, Insert(treeKey, root.toArray) :: Nil)
              } yield root
            }
        }
      }

      attempt().map { root =>
        val rootMgr = new MutableKeyValueObjectRootManager(system, kvos.pointer, treeKey, root)
        new MutableTieredKeyValueList(rootMgr)
      }
    }

    // Race condition between multiple peers simultaneously attempting to create the tree could conflict and
    // cause failures. Continually re-read and re-attempt until either creation succeeds or we see that someone
    // else created it
    system.getRetryStrategy(BasicAspenSystem.FinalizationActionRetryStrategyUUID).retryUntilSuccessful(onAttemptFailure _) {
      for {
        pool <- system.getStoragePool(poolUUID)
        kvos <- system.readObject(pool.poolDefinitionPointer)
        t <- getOrCreate(pool, kvos)
      } yield t
    }
  }
  
  class MUIterator(
      val system: AspenSystem, 
      val storeId: DataStoreID)(implicit ec: ExecutionContext) extends MissedUpdateIterator {
    
    import MissedUpdateIterator.Entry

    private[this] val ftree = loadMissedUpdateTree(system, storeId.poolUUID, storeId.poolIndex)

    private[this] var entries: List[Entry] = Nil
    private[this] val fnode = ftree.flatMap(_.fetchMutableNode(Key.AbsoluteMinimum))
    private[this] var done = false
    
    private def setDone(): Unit = done = true

    private trait NodeIter {
      def next(): Future[List[Entry]]
    }
    
    private val fiter = ftree.map { tree =>
      new NodeIter { 
        private var fnode = tree.fetchMutableNode(Key.AbsoluteMinimum)
        private var highestKey = Key.AbsoluteMinimum
        
        def load(kvos: KeyValueObjectState): List[Entry] = {
          val entries = kvos.contents.valuesIterator.map(v => Entry(byte2uuid(v.key.bytes), ObjectPointer(v.value), v.timestamp)).
            toList.filter( e => tree.keyOrdering.compare(e.objectUUID, highestKey) > 0 ).
            sortWith((a,b) => tree.keyOrdering.compare(a.objectUUID, b.objectUUID) < 0)
          if (entries.nonEmpty)
            highestKey = entries.last.objectUUID
          entries
        }
        
        def next(): Future[List[Entry]] = fnode.flatMap { node => synchronized {
          val l = load(node.kvos)
          if (l.isEmpty) {
            node.kvos.right match {
              case None =>
                setDone() 
                Future.successful(List())
              case Some(_) =>
                fnode = node.fetchRight().map(_.get).recoverWith { case _ => tree.fetchMutableNode(highestKey) }
                next()
            }
          } else
            Future.successful(l)
        }}
      }
    }
    
    def entry: Option[Entry] = synchronized { 
      entries.headOption
    }
    
    def fetchNext()(implicit ec: ExecutionContext): Future[Unit] = synchronized {
      if (done)
        Future.unit
      else {
        if (entries.isEmpty) {
          fiter.flatMap { iter =>
            iter.next().map { newEntries =>
              entries = newEntries
            }
          }
        } else {
          entries = entries.tail
          Future.unit
        }
      }
    }
    
    def markRepaired()(implicit ec: ExecutionContext): Future[Unit] = synchronized {
      if (entries.isEmpty)
        Future.unit
      else {
        system.transact { implicit tx =>
          for {
            node <- fnode
            key = Key(entries.head.objectUUID)
            reqs = List(KeyValueUpdate.KVRequirement(key, entries.head.timestamp, KeyValueUpdate.TimestampRequirement.Equals))
            _=tx.note(s"Marking repaired object ${entries.head.objectUUID} on store $storeId")
            _ <- node.prepreUpdateTransaction(Nil, List(key), reqs)
          } yield()
        }
      }
    }
  }
  
  def markMissedObject(
      system: AspenSystem, 
      obj: ObjectPointer, 
      storeIndex: Byte)(implicit ec: ExecutionContext): Future[Unit] = {
    
    println(s"**** MARKING MISSED UPDATE **** ${obj.uuid} for store $storeIndex")
    
    // Fail if the pool object has been deleted
    def onAttemptFailure(t: Throwable): Future[Unit] = t match {
      case t: CorruptedObject => throw StopRetrying(t)
    }
    
    val objKey = Key(obj.uuid)
    val value = obj.toArray
    
    system.getRetryStrategy(BasicAspenSystem.FinalizationActionRetryStrategyUUID).retryUntilSuccessful(onAttemptFailure _) {
      system.transact { implicit tx =>
        // Prevent potentially infinite recursion
        tx.disableMissedUpdateTracking()

        for {
          tl <- loadMissedUpdateTree(system, obj.poolUUID, storeIndex)
          node <- tl.fetchMutableNode(objKey)
          _=tx.note(s"Marking missed update for object ${obj.uuid} on store ${DataStoreID(obj.poolUUID, storeIndex)}")
          _ <- node.prepreUpdateTransaction(List(objKey -> value), Nil, Nil)
        } yield ()
      }
    }
  }
  
  def createHandler(
      mus: MissedUpdateStrategy, 
      system: AspenSystem,
      pointer: ObjectPointer, 
      missedStores: List[Byte])(implicit ec: ExecutionContext): MissedUpdateHandler = {

    () => Future.sequence( missedStores.map(storeIdx => markMissedObject(system, pointer, storeIdx)) ).map(_=>())

  }
  
  def createIterator(
      mus: MissedUpdateStrategy,
      system: AspenSystem,
      storeId: DataStoreID)(implicit ec: ExecutionContext): MissedUpdateIterator = new MUIterator(system, storeId)
  
}

