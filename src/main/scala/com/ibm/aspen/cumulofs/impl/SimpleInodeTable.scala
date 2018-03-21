package com.ibm.aspen.cumulofs.impl

import com.ibm.aspen.cumulofs.InodeTable
import scala.concurrent.Future
import scala.concurrent.ExecutionContext
import com.ibm.aspen.base.Transaction
import com.ibm.aspen.cumulofs.InodePointer
import com.ibm.aspen.cumulofs.InodeNumber
import com.ibm.aspen.base.tieredlist.SimpleMutableTieredKeyValueList
import com.ibm.aspen.base.tieredlist.MutableTieredKeyValueList
import com.ibm.aspen.core.transaction.KeyValueUpdate
import com.ibm.aspen.core.objects.keyvalue.Key
import com.ibm.aspen.core.objects.KeyValueObjectPointer
import scala.util.Failure
import scala.util.Success

class SimpleInodeTable(val table: SimpleMutableTieredKeyValueList) extends InodeTable {
  
  protected val rnd = new java.util.Random
  
  protected var nextInodeNumber = rnd.nextLong()
  
  protected def allocateInode(): Long = synchronized {
    val t = nextInodeNumber
    nextInodeNumber += 1
    t
  }
  
  protected def selectNewInodeAllocationPosition() = synchronized { nextInodeNumber = rnd.nextLong() }
  
  def prepareAllocation(inode: InodePointer)(implicit tx: Transaction, ec: ExecutionContext): Future[InodeNumber] = {
    val inodeNumber = allocateInode()
    
    tx.result onComplete {
      case Failure(_) => selectNewInodeAllocationPosition() // Jump to new location in case failure was due to an inode collision
      case Success(_) =>
    }
    
    table.fetchMutableNode(inodeNumber) map { node =>
      val key = Key(inodeNumber)
      val requirements = KeyValueUpdate.KVRequirement(key, tx.timestamp(), KeyValueUpdate.TimestampRequirement.DoesNotExist) :: Nil
      node.prepreUpdateTransaction(List((key, inode.toArray)), Nil, requirements)
      InodeNumber(inodeNumber)
    }
  }
  
  def lookup(inodeNumber: InodeNumber)(implicit ec: ExecutionContext): Future[Option[InodePointer]] = {
    table.get(Key(inodeNumber.number)) map { o => o match { 
      case None => None
      case Some(arr) => Some(InodePointer(KeyValueObjectPointer(arr)))
    }} 
  }
}