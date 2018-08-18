package com.ibm.aspen.base.tieredlist

import com.ibm.aspen.base.AspenSystem
import com.ibm.aspen.core.objects.keyvalue.Key
import scala.concurrent.Future
import scala.concurrent.ExecutionContext
import com.ibm.aspen.core.objects.KeyValueObjectState
import com.ibm.aspen.base.ObjectReader
import scala.concurrent.Promise
import com.ibm.aspen.core.objects.keyvalue.KeyOrdering
import com.ibm.aspen.core.objects.KeyValueObjectPointer
import scala.util.Failure
import scala.util.Success
import com.ibm.aspen.core.objects.ObjectPointer
import com.ibm.aspen.base.ObjectAllocater
import com.ibm.aspen.base.Transaction
import com.ibm.aspen.core.objects.keyvalue.Insert
import com.ibm.aspen.core.objects.keyvalue.KeyValueOperation
import com.ibm.aspen.core.objects.keyvalue.Delete
import com.ibm.aspen.core.objects.keyvalue.KeyValueObjectCodec
import com.ibm.aspen.core.objects.keyvalue.SetMin
import com.ibm.aspen.core.objects.keyvalue.SetMax
import com.ibm.aspen.core.objects.keyvalue.SetRight
import com.ibm.aspen.core.objects.keyvalue.Value
import com.ibm.aspen.core.transaction.KeyValueUpdate
import com.ibm.aspen.core.HLCTimestamp
import java.util.UUID
import com.ibm.aspen.core.objects.ObjectRevision
import com.ibm.aspen.base.RetryStrategy
import com.ibm.aspen.core.objects.keyvalue.KeyValueObjectStoreState
import com.ibm.aspen.core.objects.keyvalue.DeleteMax
import com.ibm.aspen.core.objects.keyvalue.DeleteRight


object KeyValueList {
  
  def fetchContainingNode(
      objectReader: ObjectReader, 
      listPointer: KeyValueListPointer, 
      ordering: KeyOrdering,
      key: Key)(implicit ec: ExecutionContext) : Future[KeyValueObjectState] = fetchContainingNodeWithBlacklist(objectReader, listPointer, ordering, key, Set())
        
  def fetchContainingNodeWithBlacklist(
      objectReader: ObjectReader, 
      listPointer: KeyValueListPointer, 
      ordering: KeyOrdering,
      key: Key,
      blacklist: Set[UUID])(implicit ec: ExecutionContext) : Future[KeyValueObjectState] = {
    
    // exit immediately if the requested key is below the minimum range
    if (ordering.compare(key, listPointer.minimum) < 0)
      return Future.failed(new BelowMinimumError(listPointer.minimum, key))
     
    val p = Promise[KeyValueObjectState]()
   
    def scanToContainingNode(pointer: KeyValueObjectPointer): Unit = objectReader.readObject(pointer) onComplete {
      case Failure(err) => p.failure(err)
      case Success(kvos) => 
        if (kvos.keyInRange(key, ordering))
          p.success(kvos)
        else {
          kvos.right match {
            case None => p.failure(new CorruptedLinkedList)
            case Some(right) => try {
              val next = KeyValueObjectPointer(right.content)
              if (blacklist.contains(next.uuid))
                p.success(kvos)
              else
                scanToContainingNode( next )
            } catch {
              case err: Throwable => p.failure(new CorruptedLinkedList)
            }
          }
        }
    }
   
    scanToContainingNode(listPointer.pointer)
   
    p.future
  }
  
  
  def scanToContainingNode(
      objectReader: ObjectReader, 
      initialKvos: KeyValueObjectState, 
      ordering: KeyOrdering,
      key: Key,
      blacklist: Set[UUID])(implicit ec: ExecutionContext) : Future[Option[KeyValueObjectState]] = {
    
    if (initialKvos.keyInRange(key, ordering)) {
      if (blacklist.contains(initialKvos.pointer.uuid))
        return Future.successful(None)
      else
        return Future.successful(Some(initialKvos))
    }
       
    val p = Promise[Option[KeyValueObjectState]]()
   
    def scanRight(node: KeyValueObjectState): Unit = node.right match {
      case None => p.success(Some(node))
      
      case Some(right) => 
        val rightPointer = KeyValueObjectPointer(right.content)
        
        if (node.keyInRange(key, ordering) || blacklist.contains(rightPointer.uuid))
          p.success(Some(node))
        else {
          objectReader.readObject(rightPointer) onComplete {
            case Failure(err) => p.failure(err)
            case Success(node) => scanRight(node)
          }
        }
    }
   
    scanRight(initialKvos)
   
    p.future
  }
  
  def scanToContainingNode(
      objectReader: ObjectReader, 
      initialKvos: KeyValueObjectState, 
      ordering: KeyOrdering,
      key: Key)(implicit ec: ExecutionContext) : Future[KeyValueObjectState] = {
    
    if (initialKvos.keyInRange(key, ordering))
        return Future.successful(initialKvos)
       
    val p = Promise[KeyValueObjectState]()
   
    def scanRight(node: KeyValueObjectState): Unit = node.right match {
      case None => p.success(node)
      
      case Some(right) => 
        val rightPointer = KeyValueObjectPointer(right.content)
        
        if (node.keyInRange(key, ordering))
          p.success(node)
        else {
          objectReader.readObject(rightPointer) onComplete {
            case Failure(err) => p.failure(err) 
            case Success(node) => scanRight(node)
          }
        }
    }
   
    scanRight(initialKvos)
   
    p.future
  }
  
  def prepreUpdateTransaction(
      kvos: KeyValueObjectState,
      nodeSizeLimit: Int,
      kvPairLimit: Int,
      inserts: List[(Key, Array[Byte])],
      deletes: List[Key],
      requirements: List[KeyValueUpdate.KVRequirement],
      ordering: KeyOrdering,
      reader: ObjectReader,
      allocater: ObjectAllocater,
      onSplit: (KeyValueListPointer, List[KeyValueListPointer]) => Unit,
      onJoin: (KeyValueListPointer, KeyValueListPointer) => Unit)(implicit tx: Transaction, ec: ExecutionContext): Future[Future[KeyValueObjectState]] = {
    
    if (inserts.exists(t => !kvos.keyInRange(t._1, ordering)) || deletes.exists(key => !kvos.keyInRange(key, ordering)))
       return Future.failed(new OutOfRange)
       
    val finalKVSet = (kvos.contents.keySet -- deletes.toSet) ++ inserts.map(_._1).toSet
    
    if (finalKVSet.size > 2*kvPairLimit)
      return Future.failed(new NodeSizeExceeded)
    
    val maxSize = kvos.pointer.size match {
      case None => nodeSizeLimit
      case Some(lim) => if (lim < nodeSizeLimit) lim else nodeSizeLimit
    }
    
    val sizeAfterAppend = kvos.guessSizeOnStoreAfterUpdate(inserts, deletes)
    
    if (sizeAfterAppend > 2*nodeSizeLimit)
      return Future.failed(new NodeSizeExceeded)
    
    val remaining = inserts.foldLeft(deletes.foldLeft(kvos.contents.keysIterator.toSet)((s,k) => s - k))((s,t) => s + t._1)
    
    if (sizeAfterAppend <= maxSize && remaining.size < kvPairLimit) {
      
      val updateOps = (inserts.iterator.map(t => new Insert(t._1, t._2)) ++ deletes.iterator.map(key => new Delete(key))).toList
      
      if (remaining.isEmpty)
        joinOnEmpty(kvos, requirements, updateOps, maxSize, ordering, reader, onJoin)
      else {
        
        tx.update(kvos.pointer, None, requirements, updateOps)
        
        val fnewKvos = tx.result.map(timestamp => updateState(kvos, timestamp, tx.txRevision, updateOps, kvos.maximum, kvos.right))
        
        Future.successful(fnewKvos)
      }
    } else
      split(kvos, requirements, inserts, deletes, maxSize, kvPairLimit, ordering, allocater, onSplit)
  }
  
  private def updateState(
      kvos: KeyValueObjectState,
      timestamp: HLCTimestamp,
      revision: ObjectRevision,
      ops: List[KeyValueOperation], 
      maximum: Option[KeyValueObjectState.Max], 
      right: Option[KeyValueObjectState.Right]): KeyValueObjectState = {
    
    val newContents = ops.foldLeft(kvos.contents) { (m, op) => op match {
      case i: Insert =>
        m + (i.key -> Value(i.key, i.value, timestamp, revision))
      case d: Delete => m - Key(d.value)
      case _ => m
    }}
    
    new KeyValueObjectState(kvos.pointer, kvos.revision, kvos.refcount, timestamp, timestamp,
        kvos.minimum, maximum, kvos.left, right, newContents)
  }
   
  private def split(
      kvos: KeyValueObjectState,
      requirements: List[KeyValueUpdate.KVRequirement],
      inserts: List[(Key, Array[Byte])],
      deletes: List[Key],
      maxSize: Int,
      maxPairs: Int,
      ordering: KeyOrdering,
      allocater: ObjectAllocater,
      onSplit: (KeyValueListPointer, List[KeyValueListPointer]) => Unit)(implicit tx: Transaction, ec: ExecutionContext): Future[Future[KeyValueObjectState]] = {
    
    val pruneSet = deletes.toSet
    val prunedContents = kvos.contents.filter(t => !pruneSet.contains(t._1))
    
    def getInsertOptions(key: Key): (Option[HLCTimestamp], Option[ObjectRevision]) = prunedContents.get(key) match {
      case None => (None, None)
      case Some(v) => (Some(v.timestamp), Some(v.revision))
    }
    
    val rawPairs = prunedContents.map(t => (t._1, t._2.value)) ++ inserts
    
    val keys = rawPairs.keysIterator.toArray
    
    scala.util.Sorting.quickSort(keys)(ordering)
    
    val totalKVSize = rawPairs.foldLeft(0)((sz, t) => sz + KeyValueObjectStoreState.idaEncodedPairSize(kvos.pointer.ida, t._1, t._2))
    
    def rfill(
        ops: List[KeyValueOperation], 
        insertedCount: Int,
        remainingKeys: List[Key],
        remainingCount: Int,
        remainingKVSize: Int,
        lastKey: Option[Key],  
        objectSize: Int
        ): (List[KeyValueOperation], List[Key], Key, Int, Boolean) = {
      
      if (remainingKeys.isEmpty || insertedCount == maxPairs || (!lastKey.isEmpty && (objectSize > remainingKVSize || insertedCount > remainingCount))) {
        (SetMin(lastKey.get) :: ops, remainingKeys, lastKey.get, remainingKVSize, false)
      } else {
        val key = remainingKeys.head
        val value = rawPairs(key)
        val pairSize = KeyValueObjectStoreState.idaEncodedPairSize(kvos.pointer.ida, key, value)
        val minSize = KeyValueObjectStoreState.encodedMinimumSize(key)

        if (objectSize + pairSize + minSize < maxSize) {
          val (ts, rev) = getInsertOptions(key)
          rfill(Insert(key, value, ts, rev) :: ops, insertedCount + 1, remainingKeys.tail, remainingCount - 1, remainingKVSize - pairSize, Some(key), objectSize + pairSize)
        } else
          (SetMin(lastKey.get) :: ops, remainingKeys, lastKey.get, remainingKVSize, true)
      }
    }
    
    def ralloc(
        right: Option[(Key, KeyValueObjectPointer)],
        remainingKeys: List[Key],
        remainingSize: Int,
        allocated: List[(Key, KeyValueObjectPointer)]): Future[(List[(Key, KeyValueObjectPointer)], List[Key])] = {
      
      allocater.allocateKeyValueObject(kvos.pointer, kvos.revision, Nil, None) flatMap { newPtr =>
        
        val (rops: List[KeyValueOperation], rsize) = right match {
          case None => (Nil, 0)
          case Some(t) =>
            val (max, rptr) = t
            val encodedRptr = rptr.toArray
            val rsize = KeyValueObjectStoreState.encodedMaximumSize(max) + KeyValueObjectStoreState.encodedRightSize(kvos.pointer.ida, encodedRptr)
            (SetMax(max) :: SetRight(encodedRptr) :: Nil, rsize)
        }
        
        val (ops, leftoverKeys, minimum, leftoverSize, continueAllocating) = rfill(rops, 0, remainingKeys, remainingKeys.size, remainingSize, None, rsize)
        
        tx.update(newPtr, Some(tx.txRevision), Nil, ops)
        
        if (leftoverKeys.isEmpty || !continueAllocating)
          Future.successful(((minimum, newPtr) :: allocated, Nil))
        else
          ralloc(Some((minimum, newPtr)), leftoverKeys, leftoverSize, (minimum, newPtr) :: allocated)
      }
    }
    
    val right = (kvos.right, kvos.maximum) match {
      case (Some(r), Some(m)) => Some((m.key, KeyValueObjectPointer(r.content)))
      case (None, None) => None
      case _ => return Future.failed(new CorruptedLinkedList)
    }
    
    ralloc(right, keys.toList.reverse, totalKVSize, Nil) map { t =>
      val (allocated, remainingKeys) = t
      val (newMax, newRight) = allocated.head
      val currentSet = kvos.contents.keys.toSet
      val remainingSet = remainingKeys.toSet
      val toDelete = (currentSet &~ remainingSet).toList
      val toInsert = (remainingSet &~ currentSet).toList
      val toOverwrite = (remainingSet & inserts.map(_._1).toSet).toList
      
      // Delete all keys that were previously in the object but are not in the object now
      val delOps = toDelete.map(k => Delete(k))
      val insOps = (toInsert ++ toOverwrite).map { k =>
        val (ts, rev) = getInsertOptions(k)
        Insert(k, rawPairs(k), ts, rev)
      }
      
      tx.update(kvos.pointer, Some(kvos.revision), Nil, SetMax(newMax) :: SetRight(newRight.toArray) :: (insOps ++ delOps))
      
      val leftMin = kvos.minimum match {
        case None => Key.AbsoluteMinimum
        case Some(m) => m.key
      }
      
      val left = KeyValueListPointer(leftMin, kvos.pointer)
      
      onSplit(left, allocated.map(t => KeyValueListPointer(t._1, t._2)))
      
      tx.result map { timestamp =>
        val newContents = toInsert.foldLeft(toDelete.foldLeft(kvos.contents)((m,k) => m - k)) { (m,k) =>
          m + (k -> Value(k, rawPairs(k), timestamp, tx.txRevision))
        }
        new KeyValueObjectState(kvos.pointer, tx.txRevision, kvos.refcount, timestamp, timestamp,  
          kvos.minimum, Some(KeyValueObjectState.Max(newMax, tx.txRevision, timestamp)), kvos.left,
          Some(KeyValueObjectState.Right(newRight.toArray, tx.txRevision, timestamp)), newContents)
      }
    }
  }
  
  private def joinOnEmpty(
      emptyKvos: KeyValueObjectState,
      requirements: List[KeyValueUpdate.KVRequirement],
      updateOps: List[KeyValueOperation],
      maxSize: Int,
      ordering: KeyOrdering,
      reader: ObjectReader,
      onJoin: (KeyValueListPointer, KeyValueListPointer) => Unit)(implicit tx: Transaction, ec: ExecutionContext): Future[Future[KeyValueObjectState]] = {
    
    val opsReady = emptyKvos.right match {
      case None => Future.successful(None)
      case Some(right) =>
        val rightPointer = KeyValueObjectPointer(right.content)
        reader.readObject(rightPointer).map( rightKvos => Some(rightKvos) )
    }
    
    opsReady.map { oright =>
      
      oright match {
        case None =>
          // No object to the right to pull kv pairs from. Just delete the contents and leave it empty
          tx.update(emptyKvos.pointer, Some(emptyKvos.revision), requirements, updateOps)

          tx.result.map { timestamp =>
            new KeyValueObjectState(emptyKvos.pointer, tx.txRevision, emptyKvos.refcount, timestamp, timestamp,
              emptyKvos.minimum, None, emptyKvos.left, None, Map())
          }
          
        case Some(rkvos) =>
          // Migrate all content from the right node to this node and delete the right node
          var ops = updateOps
          
          rkvos.maximum.foreach( m => ops = new SetMax(m.key) :: ops )
          rkvos.right.foreach( r => ops = new SetRight(r.content) :: ops )
          rkvos.contents.valuesIterator.foreach { v => ops = new Insert(v.key, v.value, Some(v.timestamp), Some(v.revision)) :: ops }

          tx.update(emptyKvos.pointer, Some(emptyKvos.revision), requirements, ops)
          tx.update(rkvos.pointer, Some(rkvos.revision), requirements, List())
          tx.setRefcount(rkvos.pointer, rkvos.refcount, rkvos.refcount.decrement())
          
          onJoin(KeyValueListPointer(emptyKvos), KeyValueListPointer(rkvos))
          
          tx.result.map { timestamp =>
            new KeyValueObjectState(emptyKvos.pointer, tx.txRevision, emptyKvos.refcount, timestamp, timestamp,
              emptyKvos.minimum, rkvos.maximum, emptyKvos.left, rkvos.right, rkvos.contents)
          }
      }
    }
  }
  
  def destroy(
      system: AspenSystem, 
      rootPointer: KeyValueListPointer,
      prepareForDeletion: KeyValueObjectState => Future[Unit])
      (implicit ec: ExecutionContext): Future[Unit] = system.retryStrategy.retryUntilSuccessful {
    
    def destroyRight(rootRevision: ObjectRevision, oright: Option[KeyValueObjectPointer]): Future[ObjectRevision] = oright match {
      case None => Future.successful(rootRevision)
      case Some(rp) => system.readObject(rp) flatMap { rkvos => 
        system.transact { implicit tx =>
          val rootOps = (rkvos.right, rkvos.maximum) match {
            case (None, None) => DeleteMax() :: DeleteRight() :: Nil
            case (Some(r), Some(m)) => SetRight(r.content) :: SetMax(m.key) :: Nil
            case _ => throw new CorruptedLinkedList
          }
          tx.update(rootPointer.pointer, Some(rootRevision), Nil, rootOps)
          tx.update(rkvos.pointer, Some(rkvos.revision), Nil, Nil)
          tx.setRefcount(rkvos.pointer, rkvos.refcount, rkvos.refcount.decrement())
          prepareForDeletion(rkvos).map(_ => tx.txRevision)
        }.flatMap { rootRevision => 
          destroyRight(rootRevision, rkvos.right.map(r => KeyValueObjectPointer(r.content)))
        }
      }
    }
    
    def destroyRoot(): Future[Unit] = system.readObject(rootPointer.pointer) flatMap { kvos =>
      system.transact { implicit tx =>
        tx.update(kvos.pointer, Some(kvos.revision), Nil, Nil)
        tx.setRefcount(kvos.pointer, kvos.refcount, kvos.refcount.decrement())
        Future.unit
      }
    }
    
    for {
      root <- system.readObject(rootPointer.pointer)
      _ <- destroyRight(root.revision, root.right.map(r => KeyValueObjectPointer(r.content)))
      _ <- destroyRoot()
    } yield ()
  }
}