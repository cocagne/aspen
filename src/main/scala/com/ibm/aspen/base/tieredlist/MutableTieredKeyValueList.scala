package com.ibm.aspen.base.tieredlist

import java.util.UUID

import com.ibm.aspen.base._
import com.ibm.aspen.core.{DataBuffer, HLCTimestamp}
import com.ibm.aspen.core.objects.KeyValueObjectState
import com.ibm.aspen.core.objects.keyvalue.{Key, Value}
import com.ibm.aspen.core.transaction.KeyValueUpdate
import com.ibm.aspen.core.transaction.KeyValueUpdate.TimestampRequirement
import org.apache.logging.log4j.scala.Logging

import scala.concurrent.{ExecutionContext, Future, Promise}
import scala.util.{Failure, Success}

object MutableTieredKeyValueList extends Logging {
  def load(
      system: AspenSystem,
      rootManagerType: UUID,
      serializedRootManager: DataBuffer)(implicit ec: ExecutionContext): Future[MutableTieredKeyValueList] = {
    
    system.typeRegistry.getTypeFactory[TieredKeyValueListMutableRootManagerFactory](rootManagerType) match {
      case None =>
        logger.error(s"Unregistered TieredKeyValueListMutableRootManagerFactory type: $rootManagerType")
        Future.failed(new InvalidConfiguration)
      case Some(f) =>
        f.createMutableRootManager(system, serializedRootManager) map { rootManager =>
          new MutableTieredKeyValueList(rootManager) 
        }
    }
  }
}

class MutableTieredKeyValueList(val rootManager: TieredKeyValueListMutableRootManager) extends TieredKeyValueList {
  
  def system: AspenSystem = rootManager.system
  
  val allocater: TieredKeyValueListNodeAllocater = rootManager.getAllocater()
  
  protected[tieredlist] def getObjectReaderForTier(tier: Int): ObjectReader = system

  def put(key: Key, value: Array[Byte])(implicit ec: ExecutionContext, rs: RetryStrategy): Future[Unit] = {
    rs.retryUntilSuccessful {
      system.transact { implicit tx =>
        preparePut(key, value)
      }
    }
  }

  def delete(key: Key)(implicit ec: ExecutionContext, rs: RetryStrategy): Future[Unit] = {
    rs.retryUntilSuccessful {
      system.transact { implicit tx =>
        prepareDelete(key)
      }
    }
  }
  
  def fetchMutableNode(key: Key)(implicit ec: ExecutionContext): Future[MutableTieredKeyValueListNode] = {
    fetchContainingNode(key, 0).map(kvos => new MutableTieredKeyValueListNode(system, this, kvos))
  }
  
  def preparePut(key: Key, value: Array[Byte], keyRequirement: Option[(KeyValueUpdate.TimestampRequirement.Value, HLCTimestamp)] = None)(implicit ec: ExecutionContext, tx: Transaction): Future[Unit] = for {
    node <- fetchMutableNode(key)
    reqs = keyRequirement.map(t => KeyValueUpdate.KVRequirement(key, t._2, t._1) :: Nil).getOrElse(List())
    _ <- node.prepreUpdateTransaction(List((key,value)), Nil, reqs)
  } yield {
    tx.note(s"Inserting $key into TKVL")
  }
  
  def prepareDelete(key: Key, keyRequirement: Option[(KeyValueUpdate.TimestampRequirement.Value, HLCTimestamp)] = None)(implicit ec: ExecutionContext, tx: Transaction): Future[Unit] = for {
    node <- fetchMutableNode(key)
    reqs = keyRequirement.map(t => KeyValueUpdate.KVRequirement(key, t._2, t._1) :: Nil).getOrElse(List())
    _ <- node.prepreUpdateTransaction(Nil, List(key), reqs)
  } yield {
    tx.note(s"Deleting $key from TKVL")
  }
  
  def prepareRename(oldKey: Key, newKey: Key,
                    oldKeyRequirement: Option[(KeyValueUpdate.TimestampRequirement.Value, HLCTimestamp)] = None,
                    newKeyRequirement: Option[(KeyValueUpdate.TimestampRequirement.Value, HLCTimestamp)] = None)(implicit ec: ExecutionContext, tx: Transaction): Future[Unit] = {
    val fold = fetchMutableNode(oldKey)
    val fnew = fetchMutableNode(newKey)

    val oldReqs = oldKeyRequirement.map(t => KeyValueUpdate.KVRequirement(oldKey, t._2, t._1) :: Nil).getOrElse(List())
    val newReqs = newKeyRequirement.map(t => KeyValueUpdate.KVRequirement(oldKey, t._2, t._1) :: Nil).getOrElse(List())

    def prepareUpdate(oldNode: MutableTieredKeyValueListNode, newNode: MutableTieredKeyValueListNode): Future[Unit] = {
      oldNode.kvos.contents.get(oldKey) match {
        case None => Future.failed(new KeyDoesNotExist(oldKey))
        
        case Some(v) =>
          val reqs = List(KeyValueUpdate.KVRequirement(oldKey, HLCTimestamp(0), KeyValueUpdate.TimestampRequirement.Exists))
          
          if (oldNode.kvos.pointer.uuid == newNode.kvos.pointer.uuid) {
            oldNode.prepreUpdateTransaction(List((newKey, v.value)), List(oldKey), reqs ++ oldReqs ++ newReqs).map(_=>())
          } else {
            val fo = oldNode.prepreUpdateTransaction(Nil, List(oldKey), reqs ++ oldReqs)
            val fn = newNode.prepreUpdateTransaction(List((newKey,v.value)), Nil, newReqs)
            Future.sequence(List(fo, fn)).map(_=>())
          }    
      }
    }
    
    for {
      oldNode <- fold
      newNode <- fnew
      _ <- prepareUpdate(oldNode, newNode)
    } yield {
      tx.note(s"Renaming $oldKey to $newKey in TKVL")
    }
  }
  
  /** Deletes the tree. Prior to freeing each tier0 node in the tree the prepareForDeletion method will be invoked for
   *  that node's content
   */
  def destroy(
      prepareForDeletion: Map[Key,Value] => Future[Unit])(implicit ec: ExecutionContext): Future[Unit] = system.retryStrategy.retryUntilSuccessful {
    val p = Promise[Unit]
    
    def rdelete(tiers: List[(Int, KeyValueListPointer)]): Unit = {
      if (tiers.isEmpty) {
        implicit val tx: Transaction = system.newTransaction()

        tx.note(s"Destroying TKVL")
        
        rootManager.prepareRootDeletion().foreach { _ =>
          if (tx.valid)
            p.completeWith(tx.commit().map(_=>()))
          else
            p.success(())    
        }
      } else {
        val (tier, pointer) = tiers.head
        
        def prepTier0(kvos: KeyValueObjectState): Future[Unit] = prepareForDeletion(kvos.contents)
        def prepRest(kvos: KeyValueObjectState): Future[Unit] = Future.unit
        
        val prepFunc = if (tier == 0) prepTier0 _ else prepRest _
        
        KeyValueList.destroy(system, pointer, prepFunc) foreach { _ => rdelete(tiers.tail) }
      }
    }
    
    getTierRootPointers onComplete {
      case Failure(cause) => p.failure(cause)
      case Success(tiers) => rdelete(tiers)
    }
    
    p.future
  }
  
  def getTierRootPointers()(implicit ec: ExecutionContext): Future[List[(Int, KeyValueListPointer)]] = {
    val p = Promise[List[(Int, KeyValueListPointer)]]()
    
    def rfind(ptr: KeyValueListPointer, tier: Int, tiers: List[(Int, KeyValueListPointer)]): Unit = {
      
      val thisTier = (tier, ptr) :: tiers
      
      if (tier == 0)
        p.success(thisTier)
      else {
        TieredKeyValueList.findPointerToNextTierDown(system, ptr, keyOrdering, Set(), Key.AbsoluteMinimum) onComplete {
          case Failure(_) => p.success(thisTier) // This tier must already have been deleted
          
          case Success(e) => e match {
            case Left(_) => p.success(thisTier) // Must be in-process of deleting this tier
            
            case Right(nextTierPointer) => rfind(nextTierPointer, tier - 1, thisTier)
          }
        }
      }
    }
    
    rootManager.refresh onComplete { 
      case Failure(_) => p.success(Nil)
      
      case Success(root) => rfind(KeyValueListPointer(Key.AbsoluteMinimum, root.rootNode), root.topTier, Nil)
    }
    
    p.future
  }
}