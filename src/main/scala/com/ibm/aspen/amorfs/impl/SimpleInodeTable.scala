package com.ibm.aspen.amorfs.impl

import java.util.UUID

import com.ibm.aspen.base.tieredlist.MutableTieredKeyValueList
import com.ibm.aspen.base.{AspenSystem, ObjectAllocater, Transaction}
import com.ibm.aspen.core.objects.keyvalue.Key
import com.ibm.aspen.core.transaction.KeyValueUpdate
import com.ibm.aspen.core.{DataBuffer, HLCTimestamp}
import com.ibm.aspen.amorfs.{FileType, Inode, InodePointer, InodeTable}
import com.ibm.aspen.amorfs.InodeTable.NullInode

import scala.concurrent.{ExecutionContext, Future}

class SimpleInodeTable(
    val system: AspenSystem,
    val inodeAllocaterUUID: UUID,
    val table: MutableTieredKeyValueList) extends InodeTable {
  
  protected val fallocater: Future[ObjectAllocater] = system.getObjectAllocater(inodeAllocaterUUID)
  
  protected val rnd = new java.util.Random
  
  protected var nextInodeNumber: Long = rnd.nextLong()
  
  protected def allocateInode(): Long = synchronized {
    val t = nextInodeNumber
    nextInodeNumber += 1
    t
  }
  
  protected def selectNewInodeAllocationPosition(): Unit = synchronized { 
    nextInodeNumber = rnd.nextLong()
    
    while (nextInodeNumber == NullInode)
      nextInodeNumber = rnd.nextLong()
  }
  
  def prepareInodeAllocation(inode: Inode)(implicit tx: Transaction, ec: ExecutionContext): Future[InodePointer] = {
    
    // Jump to new location if the transaction fails for any reason
    tx.result.failed.foreach( _ => selectNewInodeAllocationPosition() )
    
    val inodeNumber = allocateInode()
    val updatedInode = inode.update(inodeNumber=Some(inodeNumber))
    val key = Key(inodeNumber)
    val requirements = KeyValueUpdate.KVRequirement(key, HLCTimestamp.now, KeyValueUpdate.TimestampRequirement.DoesNotExist) :: Nil
    
    for {
      allocater <- fallocater
      node <- table.fetchMutableNode(inodeNumber)
      ptr <- allocater.allocateDataObject(node.kvos.pointer, node.kvos.revision, updatedInode.toDataBuffer)
      _=tx.note(s"Allocating new inode $inodeNumber with inode object uuid ${ptr.uuid} and file type: ${inode.fileType}")
      iptr = InodePointer(inode.fileType, inodeNumber, ptr)
      _<-node.prepreUpdateTransaction(List((key, iptr.toArray)), Nil, requirements)
    } yield {
      iptr
    }
  }
  
  def delete(pointer: InodePointer)(implicit ec: ExecutionContext): Future[Unit] = system.retryStrategy.retryUntilSuccessful {
    val key = Key(pointer.number)
    
    table.fetchMutableNode(key) flatMap { node =>
      node.kvos.contents.get(key) match {
        case None => Future.unit // already done
        
        case Some(v) =>
          val ptr = InodePointer(v.value)
          if (ptr.pointer != pointer.pointer) {
            Future.unit // Inode has been re-allocated
          } else {
            
            val requirements = KeyValueUpdate.KVRequirement(key, v.timestamp, KeyValueUpdate.TimestampRequirement.Equals) :: Nil
            
            system.transact { implicit tx =>
              tx.note(s"Deleting inode number ${pointer.number} from inode table")
              node.prepreUpdateTransaction(Nil, key :: Nil, requirements).map(_ => ())
            }
          }
      }
    }
  }
  
  def lookup(inodeNumber: Long)(implicit ec: ExecutionContext): Future[Option[InodePointer]] = {
    table.get(Key(inodeNumber)) map {
      case None => None
      case Some(arr) => Some(InodePointer(arr.value))
    }
  }
}