package com.ibm.aspen.base.tieredlist

import com.ibm.aspen.core.objects.KeyValueObjectPointer
import com.ibm.aspen.base.Transaction
import scala.concurrent.ExecutionContext
import scala.concurrent.Future
import com.ibm.aspen.core.objects.KeyValueObjectState
import com.ibm.aspen.core.objects.keyvalue.Key
import com.ibm.aspen.core.transaction.KeyValueUpdate
import com.ibm.aspen.base.ObjectAllocater
import com.ibm.aspen.base.AspenSystem
import com.ibm.aspen.core.objects.keyvalue.Value
import com.ibm.aspen.core.HLCTimestamp
import com.ibm.aspen.core.objects.keyvalue.KeyOrdering

trait MutableTieredKeyValueList extends TieredKeyValueList {
  
  val system: AspenSystem
  
  protected val treeIdentifier: Key
  protected val treeContainer: Either[KeyValueObjectPointer, TieredKeyValueList.Root]
  
  protected def getObjectAllocaterForTier(tier: Int)(implicit ec: ExecutionContext): Future[ObjectAllocater]
  
  /** Future completes when the transactions is ready to commit */
  protected[tieredlist] def prepreUpdateRootTransaction(
      newTier: Int,
      newRootPointer: KeyValueObjectPointer)(implicit tx: Transaction, ec: ExecutionContext): Future[Unit]
  
  
  protected[tieredlist] def refreshRoot()(implicit ec: ExecutionContext): Future[(KeyValueObjectState, TieredKeyValueList.Root)]
  
  def fetchRoot()(implicit ec: ExecutionContext): Future[TieredKeyValueList.Root] = refreshRoot().map(t => t._2)
  
  /** Delete's the tree. Prior to freeing each tier0 node in the tree the prepareForDeletion method will be invoked for
   *  that node's content
   */
  def destroy(prepareForDeletion: Map[Key,Value] => Future[Unit])(implicit ec: ExecutionContext): Future[Unit]
  
  class MutableNode(val kvos: KeyValueObjectState) {
    
    def ordering: KeyOrdering = keyOrdering
    
    /** Note that this future will fail if the right node node has been deleted */
    def fetchRight()(implicit ec: ExecutionContext): Future[Option[MutableNode]] = kvos.right match {
      case None => Future.successful(None)
      case Some(ptr) => system.readObject(KeyValueObjectPointer(ptr.content)).map(kvos => Some(new MutableNode(kvos)))
    }
    
    /** Note that this future will fail if the node has been deleted */
    def refresh()(implicit ec: ExecutionContext): Future[MutableNode] = system.readObject(kvos.pointer).map(kv => new MutableNode(kv))
    
    def prepreUpdateTransaction(
      inserts: List[(Key, Array[Byte])],
      deletes: List[Key],
      requirements: List[KeyValueUpdate.KVRequirement])(implicit tx: Transaction, ec: ExecutionContext): Future[Future[MutableNode]] = {
      
      val reader = getObjectReaderForTier(0) 
      
      def onSplit(left: KeyValueListPointer, right: List[KeyValueListPointer]): Unit = {
        TieredKeyValueListSplitFA.addFinalizationAction(tx, treeIdentifier, treeContainer, keyOrdering, 1, left, right)
      }
      
      def onJoin(left: KeyValueListPointer, removed: KeyValueListPointer): Unit = {
        TieredKeyValueListJoinFA.addFinalizationAction(tx, treeIdentifier, treeContainer, keyOrdering, 1, left, removed)
      }
      
      val fallocater = getObjectAllocaterForTier(0)
      
      for {
        allocater <- fallocater
        root <- rootPointer()
        fupdatedKvos <- KeyValueList.prepreUpdateTransaction(kvos, root.getTierNodeSize(0), root.getTierNodeKVPairLimit(0), inserts, deletes, requirements, 
                       keyOrdering, reader, allocater, onSplit, onJoin)
      } yield {
        tx.result.flatMap(_ => fupdatedKvos.map(updatedKvos => new MutableNode(updatedKvos)))
      }
    }
  }
  
  def fetchMutableNode(key: Key)(implicit ec: ExecutionContext): Future[MutableNode] = fetchContainingNode(key, 0).map(new MutableNode(_))
  
  def put(key: Key, value: Array[Byte])(implicit ec: ExecutionContext, t: Transaction): Future[Unit] = for {
    node <- fetchMutableNode(key) 
    prep <- node.prepreUpdateTransaction(List((key,value)), Nil, Nil)
  } yield ()
  
  def delete(key: Key)(implicit ec: ExecutionContext, t: Transaction): Future[Unit] = for {
    node <- fetchMutableNode(key) 
    prep <- node.prepreUpdateTransaction(Nil, List(key), Nil)
  } yield ()
  
  def replace(oldKey: Key, newKey: Key)(implicit ec: ExecutionContext, t: Transaction): Future[Unit] = {
    val fold = fetchMutableNode(oldKey)
    val fnew = fetchMutableNode(newKey)
    
    def prepareUpdate(oldNode: MutableNode, newNode: MutableNode): Future[Unit] = {
      oldNode.kvos.contents.get(oldKey) match {
        case None => Future.failed(new KeyDoesNotExist(oldKey))
        
        case Some(v) =>
          val reqs = List(KeyValueUpdate.KVRequirement(oldKey, HLCTimestamp(0), KeyValueUpdate.TimestampRequirement.Exists))
          
          if (oldNode.kvos.pointer.uuid == newNode.kvos.pointer.uuid) {
            oldNode.prepreUpdateTransaction(List((newKey, v.value)), List(oldKey), reqs).map(_=>())
          } else {
            val fo = oldNode.prepreUpdateTransaction(Nil, List(oldKey), reqs)
            val fn = newNode.prepreUpdateTransaction(List((newKey,v.value)), Nil, Nil)
            Future.sequence(List(fo, fn)).map(_=>())
          }    
      }
    }
    
    for {
      oldNode <- fold
      newNode <- fnew
      prep <- prepareUpdate(oldNode, newNode)
    } yield ()
  }
}