package com.ibm.aspen.base.tieredlist

import com.ibm.aspen.base.ObjectReader
import com.ibm.aspen.core.objects.KeyValueObjectPointer
import com.ibm.aspen.core.objects.keyvalue.Key
import com.ibm.aspen.base.AspenSystem
import java.util.UUID
import com.ibm.aspen.base.Transaction
import scala.concurrent.ExecutionContext
import scala.concurrent.Future
import com.ibm.aspen.core.DataBuffer
import com.ibm.aspen.util.Varint
import java.nio.ByteBuffer
import com.ibm.aspen.core.transaction.KeyValueUpdate
import com.ibm.aspen.core.objects.keyvalue.Insert
import com.ibm.aspen.core.objects.keyvalue.Delete
import com.ibm.aspen.core.objects.KeyValueObjectState

class MutableKeyValueObjectRootManager(
    val system: AspenSystem,
    containingObject: KeyValueObjectPointer,
    treeKey: Key,
    initialRoot: TieredKeyValueListRoot) extends KeyValueObjectRootManager(system, containingObject, treeKey, initialRoot) with TieredKeyValueListMutableRootManager {

  val typeUUID: UUID = MutableKeyValueObjectRootManager.typeUUID
  
  def serialize(): Array[Byte] = {
    val arr = new Array[Byte](Varint.getUnsignedIntEncodingLength(treeKey.bytes.length) + treeKey.bytes.length + containingObject.encodedSize)
    val bb = ByteBuffer.wrap(arr)
    Varint.putUnsignedInt(bb, treeKey.bytes.length)
    bb.put(treeKey.bytes)
    containingObject.encodeInto(bb)
    arr
  }
  
  def prepareRootUpdate(
      newRootTier: Int,
      allocater: TieredKeyValueListNodeAllocater,
      inserted: List[KeyValueListPointer])(implicit tx: Transaction, ec: ExecutionContext): Future[Unit] = {

    val fkvos = reader.readSingleKey(containingObject, treeKey, root.keyOrdering)
    val falloc = allocater.tierNodeAllocater(newRootTier)
    val iops = Insert(Key.AbsoluteMinimum, root.rootNode.toArray) :: inserted.map(p => Insert(p.minimum, p.pointer.toArray))
    
    for {
      kvos <- fkvos
      allocater <- falloc
      // FIXME - switch to revision guard on key
      newRootPointer <- allocater.allocateKeyValueObject(kvos.pointer, kvos.revision, iops)
    } yield {
      kvos.contents.get(treeKey) match {
        case None => throw new InvalidRoot
        
        case Some(v) =>
          val currentRoot = TieredKeyValueListRoot(v.value)
          if (currentRoot.topTier >= newRootTier)
            throw new TierAlreadyCreated
          val newRoot = currentRoot.copy(topTier = newRootTier, rootNode = newRootPointer)
          val req = KeyValueUpdate.KVRequirement(treeKey, v.timestamp, KeyValueUpdate.TimestampRequirement.Equals) :: Nil

          tx.update(containingObject, Some(kvos.revision), req, Insert(treeKey, newRoot.toArray) :: Nil)
          
          tx.result.foreach { _ => synchronized {
            troot = newRoot
          }}
      }
    }
  }
  
  def prepareRootDeletion()(implicit tx: Transaction, ec: ExecutionContext): Future[Unit] = {
    reader.readSingleKey(containingObject, treeKey, root.keyOrdering) map { kvos =>
      kvos.contents.get(treeKey) map { v =>
        val reqs = KeyValueUpdate.KVRequirement(treeKey, v.timestamp, KeyValueUpdate.TimestampRequirement.Equals) :: Nil
        tx.update(containingObject, None, reqs, Delete(treeKey) :: Nil)
      }
    }
  }
}

object MutableKeyValueObjectRootManager extends TieredKeyValueListMutableRootManagerFactory {
  val typeUUID: UUID = UUID.fromString("a024defb-5001-4dfd-8676-57796fe982fa")
  
  def apply(system: AspenSystem, kvos: KeyValueObjectState, rootKey: Key): MutableKeyValueObjectRootManager = {
    new MutableKeyValueObjectRootManager(system, kvos.pointer, rootKey, TieredKeyValueListRoot(kvos.contents(rootKey).value))
  }
  
  def createMutableRootManager(
      system: AspenSystem, 
      serializedRootManager: DataBuffer)(implicit ec: ExecutionContext): Future[TieredKeyValueListMutableRootManager] = {
    
    val bb = serializedRootManager.asReadOnlyBuffer()
    val keyLen = Varint.getUnsignedInt(bb)
    val keyArr = new Array[Byte](keyLen)
    bb.get(keyArr)
    val treeKey = Key(keyArr)
    val containingObject = KeyValueObjectPointer(bb)
    
    system.readObject(containingObject) map { kvos =>
      kvos.contents.get(treeKey) match {
        case None => throw new InvalidRoot
        case Some(v) =>
          val initialRoot = TieredKeyValueListRoot(v.value)
          new MutableKeyValueObjectRootManager(system, containingObject, treeKey, initialRoot)
      }
    }
  }
}