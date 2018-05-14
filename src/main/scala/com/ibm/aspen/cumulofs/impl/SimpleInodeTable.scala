package com.ibm.aspen.cumulofs.impl

import com.ibm.aspen.cumulofs.InodeTable
import scala.concurrent.Future
import scala.concurrent.ExecutionContext
import com.ibm.aspen.base.Transaction
import com.ibm.aspen.cumulofs.InodePointer
import com.ibm.aspen.base.tieredlist.SimpleMutableTieredKeyValueList
import com.ibm.aspen.base.tieredlist.MutableTieredKeyValueList
import com.ibm.aspen.core.transaction.KeyValueUpdate
import com.ibm.aspen.core.objects.keyvalue.Key
import com.ibm.aspen.core.objects.KeyValueObjectPointer
import scala.util.Failure
import scala.util.Success
import com.ibm.aspen.cumulofs.FileType
import com.ibm.aspen.base.AspenSystem
import java.util.UUID
import com.ibm.aspen.core.objects.keyvalue.KeyValueOperation
import com.ibm.aspen.cumulofs.InodeTable.NullInode

class SimpleInodeTable(
    val system: AspenSystem,
    val inodeAllocaterUUID: UUID,
    val table: SimpleMutableTieredKeyValueList) extends InodeTable {
  
  val fallocater = system.getObjectAllocater(inodeAllocaterUUID)
  
  protected val rnd = new java.util.Random
  
  protected var nextInodeNumber = rnd.nextLong()
  
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
  
  def prepareInodeAllocation(
      ftype: FileType.Value, 
      inodeOps: List[KeyValueOperation])(implicit tx: Transaction, ec: ExecutionContext): Future[InodePointer] = {
    
    // Jump to new location if the transaction fails for any reason
    tx.result.failed.foreach( _ => selectNewInodeAllocationPosition() )
    
    val inodeNumber = allocateInode()
    val key = Key(inodeNumber)
    val requirements = KeyValueUpdate.KVRequirement(key, tx.timestamp(), KeyValueUpdate.TimestampRequirement.DoesNotExist) :: Nil
    
    for {
      allocater <- fallocater
      node <- table.fetchMutableNode(inodeNumber)
      ptr <- allocater.allocateKeyValueObject(node.kvos.pointer, node.kvos.revision, inodeOps)
    } yield {
      val iptr = InodePointer(ftype, inodeNumber, ptr)
      node.prepreUpdateTransaction(List((key, iptr.toArray)), Nil, requirements)
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
              node.prepreUpdateTransaction(Nil, key :: Nil, requirements).map(_ => ())
            }
          }
      }
    }
  }
  
  def lookup(inodeNumber: Long)(implicit ec: ExecutionContext): Future[Option[InodePointer]] = {
    table.get(Key(inodeNumber)) map { o => o match { 
      case None => None
      case Some(arr) => Some(InodePointer(arr.value))
    }} 
  }
}