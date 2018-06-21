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
            case Some(arr) => try {
              val next = ObjectPointer.fromArray(arr).asInstanceOf[KeyValueObjectPointer]
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
      
      case Some(arr) => 
        val rightPointer = ObjectPointer.fromArray(arr).asInstanceOf[KeyValueObjectPointer]
        
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
      
      case Some(arr) => 
        val rightPointer = ObjectPointer.fromArray(arr).asInstanceOf[KeyValueObjectPointer]
        
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
      inserts: List[(Key, Array[Byte])],
      deletes: List[Key],
      requirements: List[KeyValueUpdate.KVRequirement],
      ordering: KeyOrdering,
      reader: ObjectReader,
      allocater: ObjectAllocater,
      onSplit: (KeyValueListPointer, KeyValueListPointer) => Unit,
      onJoin: (KeyValueListPointer, KeyValueListPointer) => Unit)(implicit tx: Transaction, ec: ExecutionContext): Future[KeyValueObjectState] = {
    
    if (inserts.exists(t => !kvos.keyInRange(t._1, ordering)) || deletes.exists(key => !kvos.keyInRange(key, ordering)))
       return Future.failed(new OutOfRange)
   
    val timestamp = tx.timestamp()
    
    val appendOps = (inserts.iterator.map(t => new Insert(t._1.bytes, t._2, timestamp)) ++ deletes.iterator.map(key => new Delete(key.bytes))).toList
    
    val maxSize = kvos.pointer.size match {
      case None => nodeSizeLimit
      case Some(lim) => if (lim < nodeSizeLimit) lim else nodeSizeLimit
    }
    
    val sizeAfterAppend = kvos.sizeOnStore + KeyValueObjectCodec.calculateEncodedSize(kvos.pointer.ida, appendOps) 
    
    if (sizeAfterAppend <= maxSize) {
      
      val newKvos = updateState(kvos, appendOps, sizeAfterAppend, kvos.maximum, kvos.right)
      
      if (newKvos.contents.isEmpty)
        joinOnEmpty(newKvos, requirements, timestamp, maxSize, ordering, reader, onJoin)
      else {
        
        tx.append(kvos.pointer, None, requirements, appendOps)
        
        Future.successful(newKvos)
      }
    } else {
      
      val deleteSet = deletes.toSet
      var ops = List[KeyValueOperation]()
      
      inserts.foreach(t => ops = new Insert(t._1.bytes, t._2, timestamp) :: ops)
      kvos.contents.foreach { t => 
        if (!deleteSet.contains(t._1))
          ops = new Insert(t._1.bytes, t._2.value, t._2.timestamp) :: ops
      }

      kvos.minimum.foreach( arr => ops = new SetMin(arr) :: ops )
      kvos.maximum.foreach( arr => ops = new SetMax(arr) :: ops )
      kvos.right.foreach( arr => ops = new SetRight(arr) :: ops )
      
      if (KeyValueObjectCodec.calculateEncodedSize(kvos.pointer.ida, ops) <= maxSize) {
        val newKvos = updateState(kvos, appendOps, sizeAfterAppend, kvos.maximum, kvos.right)
        
        if (newKvos.contents.isEmpty)
          joinOnEmpty(newKvos, requirements, timestamp, maxSize, ordering, reader, onJoin)
        else {
          tx.overwrite(kvos.pointer, kvos.revision, requirements, ops)
          Future.successful(newKvos)
        }
      } else
        split(kvos, requirements, inserts, deleteSet, timestamp, maxSize, ordering, allocater, onSplit)
    }
  }
  
  private def updateState(
      kvos: KeyValueObjectState, 
      ops: List[KeyValueOperation], 
      newSizeOnStore: Int, 
      maximum: Option[Key], 
      right: Option[Array[Byte]]): KeyValueObjectState = {
    
    val newContents = ops.foldLeft(kvos.contents) { (m, op) => op match {
      case i: Insert =>
        val key = Key(i.key)
        m + (key -> Value(key, i.value, i.timestamp))
      case d: Delete => m - Key(d.value)
      case _ => m
    }}
    
    new KeyValueObjectState(kvos.pointer, kvos.revision, kvos.refcount, kvos.timestamp, newSizeOnStore, 
        kvos.minimum, maximum, kvos.left, right, newContents)
  }
   
  private def split(
      kvos: KeyValueObjectState,
      requirements: List[KeyValueUpdate.KVRequirement],
      inserts: List[(Key, Array[Byte])],
      deleteSet: Set[Key],
      timestamp: HLCTimestamp,
      maxSize: Int,
      ordering: KeyOrdering,
      allocater: ObjectAllocater,
      onSplit: (KeyValueListPointer, KeyValueListPointer) => Unit)(implicit tx: Transaction, ec: ExecutionContext): Future[KeyValueObjectState] = {
    
    val contents = inserts.foldLeft(kvos.contents.filter(t => !deleteSet.contains(t._1))){ (m, t) =>
      m + (t._1 -> Value(t._1, t._2, timestamp))
    }

    val keys = contents.keysIterator.toArray
    
    scala.util.Sorting.quickSort(keys)(ordering)
    
    var rightIndex = keys.length / 2
    var rightMin = keys(rightIndex)
    
    var leftOps = List[KeyValueOperation]()
    var rightOps = List[KeyValueOperation]()
    
    for (i <- 0 until keys.length) {
      val v = contents(keys(i))
      val ins = new Insert(v.key.bytes, v.value, v.timestamp)
      
      if ( i < rightIndex )
        leftOps = ins :: leftOps
      else
        rightOps = ins :: rightOps
    }
    
    kvos.maximum.foreach( arr => rightOps = new SetMax(arr) :: rightOps )
    kvos.right.foreach( arr => rightOps = new SetRight(arr) :: rightOps )
    rightOps = new SetMin(rightMin) :: rightOps

    allocater.allocateKeyValueObject(kvos.pointer, kvos.revision, rightOps, None) map { rightNodePointer =>
      val rnpArr = rightNodePointer.toArray
      leftOps = new SetMax(rightMin) :: leftOps
      leftOps = new SetRight(rnpArr) :: leftOps
      val newSizeOnStore = KeyValueObjectCodec.calculateEncodedSize(kvos.pointer.ida, leftOps)
      
      tx.overwrite(kvos.pointer, kvos.revision, requirements, leftOps)
      
      val left = KeyValueListPointer(kvos.minimum.getOrElse(Key.AbsoluteMinimum), kvos.pointer)
      val right = KeyValueListPointer(rightMin, rightNodePointer)
      
      onSplit(left, right)
      
      new KeyValueObjectState(kvos.pointer, tx.txRevision, kvos.refcount, timestamp, newSizeOnStore, 
        kvos.minimum, Some(rightMin), kvos.left, Some(rnpArr), contents)
    }
  }
  
  private def joinOnEmpty(
      emptyKvos: KeyValueObjectState,
      requirements: List[KeyValueUpdate.KVRequirement],
      timestamp: HLCTimestamp,
      maxSize: Int,
      ordering: KeyOrdering,
      reader: ObjectReader,
      onJoin: (KeyValueListPointer, KeyValueListPointer) => Unit)(implicit tx: Transaction, ec: ExecutionContext): Future[KeyValueObjectState] = {
    
    val opsReady = emptyKvos.right match {
      case None => Future.successful(None)
      case Some(rightArr) =>
        val rightPointer = ObjectPointer.fromArray(rightArr).asInstanceOf[KeyValueObjectPointer]
        reader.readObject(rightPointer).map( rightKvos => Some(rightKvos) )
    }
    
    opsReady.map { oright =>
      var ops = List[KeyValueOperation]()
      
      emptyKvos.minimum.foreach( arr => ops = new SetMin(arr) :: ops )
      
      oright match {
        case None => 
          tx.overwrite(emptyKvos.pointer, emptyKvos.revision, requirements, ops)
          val newSizeOnStore = KeyValueObjectCodec.calculateEncodedSize(emptyKvos.pointer.ida, ops)
          new KeyValueObjectState(emptyKvos.pointer, tx.txRevision, emptyKvos.refcount, timestamp, newSizeOnStore, 
            emptyKvos.minimum, None, emptyKvos.left, None, Map())
          
        case Some(rkvos) =>
          rkvos.maximum.foreach( x => ops = new SetMax(x) :: ops )
          rkvos.right.foreach( x => ops = new SetRight(x) :: ops )
          rkvos.contents.foreach { t => ops = new Insert(t._1.bytes, t._2.value, t._2.timestamp) :: ops }
          val newSizeOnStore = KeyValueObjectCodec.calculateEncodedSize(emptyKvos.pointer.ida, ops)
          
          tx.overwrite(emptyKvos.pointer, emptyKvos.revision, requirements, ops)
          tx.overwrite(rkvos.pointer, rkvos.revision, requirements, List())
          tx.setRefcount(rkvos.pointer, rkvos.refcount, rkvos.refcount.decrement())
          
          onJoin(KeyValueListPointer(emptyKvos), KeyValueListPointer(rkvos))
          
          new KeyValueObjectState(emptyKvos.pointer, tx.txRevision, emptyKvos.refcount, timestamp, newSizeOnStore, 
            emptyKvos.minimum, rkvos.maximum, emptyKvos.left, rkvos.right, Map())
      }
    }
  }
  
  def destroy(
      system: AspenSystem, 
      rootPointer: KeyValueListPointer,
      prepareForDeletion: KeyValueObjectState => Future[Unit])
      (implicit ec: ExecutionContext): Future[Unit] = system.retryStrategy.retryUntilSuccessful {
   
    val p = Promise[Unit]()
    
    def getOverwriteOps(kvos: KeyValueObjectState): List[KeyValueOperation] = kvos.right match {
       case None => Nil
       case Some(arr) => SetRight(arr) :: Nil
    }
    
    // Discard all content in the root node except the right pointer
    def prepRoot(initialRoot: KeyValueObjectState): Future[Unit] = system.transact { implicit tx =>
      for {
        _ <- prepareForDeletion(initialRoot)
        _ = tx.overwrite(rootPointer.pointer, initialRoot.revision, Nil, getOverwriteOps(initialRoot))   
      } yield ()
    }
    
    // Delete the node to the right of the root if it exists. If not, delete the root node
    def unlink(rootRevision: ObjectRevision, oright: Option[Array[Byte]]): Unit = oright match {
      case None => 
        val fcomplete = system.transact { implicit tx =>
          for {
            kvos <- system.readObject(rootPointer.pointer)
            _ = tx.setRefcount(rootPointer.pointer, kvos.refcount, kvos.refcount.decrement())
          } yield ()
        }
        
        p.completeWith(fcomplete)
        
      case Some(arr) => 
        val funlink = system.transact { implicit tx =>
          for {
            rkvos <- system.readObject(KeyValueObjectPointer(arr))
            _ <- prepareForDeletion(rkvos)
            _ = tx.overwrite(rootPointer.pointer, rootRevision, Nil, getOverwriteOps(rkvos))
            _ = tx.setRefcount(rkvos.pointer, rkvos.refcount, rkvos.refcount.decrement())
          } yield (tx.txRevision, rkvos.right)
        }
        
        funlink onComplete {
          case Failure(err) => p.failure(err)
          case Success((newRevision, oright)) => unlink(newRevision, oright)
        }
    }
    
    system.readObject(rootPointer.pointer) onComplete {
      case Success(initialRoot) => 
        val finit = for {
          _ <- prepRoot(initialRoot)
          refreshedRoot <- system.readObject(rootPointer.pointer)
        } yield {
          unlink(refreshedRoot.revision, refreshedRoot.right)
        }
        
        finit.failed.foreach( err => p.failure(err) )
         
      case Failure(_) =>
        // Failing to read the root node should mean that it has already been deleted.
        p.success(())
    }
    
    p.future
  }
}