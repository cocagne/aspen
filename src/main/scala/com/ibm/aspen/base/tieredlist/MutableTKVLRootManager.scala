package com.ibm.aspen.base.tieredlist

import java.util.UUID
import com.ibm.aspen.base.AspenSystem
import com.ibm.aspen.core.objects.keyvalue.Key
import com.ibm.aspen.util.Varint
import java.nio.ByteBuffer
import com.ibm.aspen.core.DataBuffer
import scala.concurrent.ExecutionContext
import scala.concurrent.Future
import com.ibm.aspen.base.Transaction
import com.ibm.aspen.core.transaction.KeyValueUpdate
import com.ibm.aspen.core.objects.keyvalue.Insert

class MutableTKVLRootManager(
    val system: AspenSystem,
    containingTKVL: MutableTieredKeyValueList,
    treeKey: Key,
    initialRoot: TieredKeyValueListRoot) extends TKVLRootManager(system, containingTKVL, treeKey, initialRoot) with TieredKeyValueListMutableRootManager {
  
  val typeUUID: UUID = MutableKeyValueObjectRootManager.typeUUID
  
  def serialize(): Array[Byte] = {
    val serializedContainerRootManager = containingTKVL.rootManager.serialize()
    
    val sz = 16 + Varint.getUnsignedIntEncodingLength(treeKey.bytes.length) + treeKey.bytes.length + serializedContainerRootManager.size
             
    val arr = new Array[Byte](sz)
    val bb = ByteBuffer.wrap(arr)
    
    bb.putLong(containingTKVL.rootManager.typeUUID.getMostSignificantBits)
    bb.putLong(containingTKVL.rootManager.typeUUID.getLeastSignificantBits)
    Varint.putUnsignedInt(bb, treeKey.bytes.length)
    bb.put(treeKey.bytes)
    bb.put(serializedContainerRootManager)

    arr
  }
  
  def prepareRootUpdate(
      newRootTier: Int,
      allocater: TieredKeyValueListNodeAllocater,
      inserted: List[KeyValueListPointer])(implicit tx: Transaction, ec: ExecutionContext): Future[Unit] = {
    
    val fmn = containingTKVL.fetchMutableNode(treeKey)
    val falloc = allocater.tierNodeAllocater(newRootTier)
    val iops = Insert(Key.AbsoluteMinimum, root.rootNode.toArray) :: inserted.map(p => Insert(p.minimum, p.pointer.toArray))
    
    for {
      mn <- fmn
      allocater <- falloc
      // FIXME - switch to revision guard on key
      newRootPointer <- allocater.allocateKeyValueObject(mn.kvos.pointer, mn.kvos.revision, iops)
    } yield {
      mn.kvos.contents.get(treeKey) match {
        case None => throw new InvalidRoot
        
        case Some(v) =>
          val currentRoot = TieredKeyValueListRoot(v.value)
          if (currentRoot.topTier >= newRootTier)
            throw new TierAlreadyCreated
          val newRoot = currentRoot.copy(topTier = newRootTier, rootNode = newRootPointer)
          val req = KeyValueUpdate.KVRequirement(treeKey, v.timestamp, KeyValueUpdate.TimestampRequirement.Equals) :: Nil
          
          mn.prepreUpdateTransaction((treeKey -> newRoot.toArray) :: Nil, Nil, req)
          
          tx.result.foreach { _ => synchronized {
            troot = newRoot
          }}
      }
    }
  }
  
  def prepareRootDeletion()(implicit tx: Transaction, ec: ExecutionContext): Future[Unit] = {
    containingTKVL.fetchMutableNode(treeKey) map { mn =>
      mn.kvos.contents.get(treeKey) map { v =>
        val reqs = KeyValueUpdate.KVRequirement(treeKey, v.timestamp, KeyValueUpdate.TimestampRequirement.Equals) :: Nil
        mn.prepreUpdateTransaction(Nil, treeKey :: Nil, reqs)
      }
    }
  }
}

object MutableTKVLRootManager extends TieredKeyValueListMutableRootManagerFactory {
  val typeUUID = UUID.fromString("847b0dbd-6d49-4926-a109-165ba32a68cb")
  
  def load( 
      containingTKVL: MutableTieredKeyValueList, 
      rootKey: Key)(implicit ec: ExecutionContext): Future[Option[MutableTKVLRootManager]] = {
    containingTKVL.get(rootKey).map(ov => ov.map(v => new MutableTKVLRootManager(containingTKVL.system, containingTKVL, rootKey, TieredKeyValueListRoot(v.value))))
  }
  
  def createMutableRootManager(
      system: AspenSystem, 
      serializedRootManager: DataBuffer)(implicit ec: ExecutionContext): Future[TieredKeyValueListMutableRootManager] = {
    
    val bb = serializedRootManager.asReadOnlyBuffer()
    val msb = bb.getLong()
    val lsb = bb.getLong()
    val mtkvlRootManagerType = new UUID(msb, lsb)
    val keyLen = Varint.getUnsignedInt(bb)
    val keyArr = new Array[Byte](keyLen)
    bb.get(keyArr)
    
    val treeKey = Key(keyArr)
    
    // remaining data in buffer is the serialized root manager for the containing tkvl
    system.typeRegistry.getTypeFactory[TieredKeyValueListMutableRootManagerFactory](mtkvlRootManagerType) match {
      case None => Future.failed(new InvalidConfiguration)
      case Some(f) => 
        f.createMutableRootManager(system, DataBuffer(bb)).flatMap { containingTKVLRootManager =>
          val containingTKVL = new MutableTieredKeyValueList(containingTKVLRootManager)
          containingTKVL.get(treeKey).map { ov => ov match {
            case None => throw new InvalidRoot
            case Some(v) =>
              val initialRoot = TieredKeyValueListRoot(v.value)
              new MutableTKVLRootManager(system, containingTKVL, treeKey, initialRoot)
          }}
        }
    }
  }
}