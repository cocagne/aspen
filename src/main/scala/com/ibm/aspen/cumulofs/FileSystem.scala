package com.ibm.aspen.cumulofs

import com.ibm.aspen.base.AspenSystem
import com.ibm.aspen.core.objects.ObjectPointer
import com.ibm.aspen.core.objects.ObjectRevision
import com.ibm.aspen.base.Transaction
import scala.concurrent.ExecutionContext
import scala.concurrent.Future
import com.ibm.aspen.base.ObjectAllocater
import com.ibm.aspen.core.objects.KeyValueObjectPointer
import com.ibm.aspen.core.objects.keyvalue.KeyValueOperation
import com.ibm.aspen.core.objects.keyvalue.Key
import java.util.UUID
import com.ibm.aspen.core.objects.keyvalue.IntegerKeyOrdering
import com.ibm.aspen.base.tieredlist.TieredKeyValueList

trait FileSystem {
  val system: AspenSystem
  
  val inodeTable: InodeTable
  
  val inodeLoader: InodeLoader
  
  val directoryLoader: DirectoryLoader
  
  def loadDirectory(pointer: DirectoryPointer): Directory = directoryLoader.loadDirectory(this, pointer)
}

object FileSystem {
  
  val InodeTableKey                    = Key(0)
  val DirectoryTableAllocatersArrayKey = Key(1)
  val DirectoryTableSizesKey           = Key(2)
  val DataTableAllocatersArrayKey      = Key(3)
  val DataTableSizesKey                = Key(4)
  
  /** Creates a new CumuloFS file system as part of the supplied Transaction.
   *  
   *  @returns Pointer to the file system root object
   *  
   *  Involves 3 simultaneous allocations
   *    - FS root object
   *    - First leaf of the Inode Table
   *    - Root directory Inode 
   */
  def prepareNewFileSystem(
      allocatingObject: ObjectPointer,
      allocatingObjectRevision: ObjectRevision,
      allocater: ObjectAllocater,
      inodeTableAllocaters: Array[UUID],     // For InodeTable Tiered List
      inodeTableSizes: Array[Int],
      directoryTableAllocaters: Array[UUID], // For Directory entry Tiered List
      directoryTableSizes: Array[Int],
      dataTableAllocaters: Array[UUID],      // For File Data Tiered List
      dataTableSizes: Array[Int]
      )(implicit tx: Transaction, ec: ExecutionContext): Future[KeyValueObjectPointer] = {
    
    import FileMode._
    
    val rootDirMode = S_IRWXU | S_IRWXG | S_IROTH | S_IXOTH  
    
    val (rootOps, rootContent) = DirectoryInode.getInitialContent(rootDirMode, 0, 0, None)
    
    for {
      rootDirObj <- allocater.allocateKeyValueObject(allocatingObject, allocatingObjectRevision, rootOps)
      
      rootDirPtr = new DirectoryPointer(0, rootDirObj)
      
      inodeTblContent = KeyValueOperation.insertOperations(List((Key(0), rootDirPtr.toArray)), tx.timestamp())
      
      rootInodeTblPtr <- allocater.allocateKeyValueObject(allocatingObject, allocatingObjectRevision, inodeTblContent)
      
      inodeTblRoot = new TieredKeyValueList.Root(0, inodeTableAllocaters, inodeTableSizes, IntegerKeyOrdering, rootInodeTblPtr)
      
      icontent = List(
          (InodeTableKey,                    inodeTblRoot.toArray),
          (DirectoryTableAllocatersArrayKey, encodeUUIDArray(directoryTableAllocaters)),
          (DirectoryTableSizesKey,           encodeIntArray(directoryTableSizes)),
          (DataTableAllocatersArrayKey,      encodeUUIDArray(dataTableAllocaters)),
          (DataTableSizesKey,                encodeIntArray(dataTableSizes)))
      
      fsObjContent = KeyValueOperation.insertOperations(icontent, tx.timestamp())
      
      fsObjPtr <- allocater.allocateKeyValueObject(allocatingObject, allocatingObjectRevision, fsObjContent)
      
    } yield fsObjPtr
  }
}