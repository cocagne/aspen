package com.ibm.aspen.base.impl

import java.util.UUID
import com.ibm.aspen.base.Transaction
import com.ibm.aspen.core.objects.ObjectPointer
import com.google.flatbuffers.FlatBufferBuilder
import com.ibm.aspen.base.impl.{codec => P}
import com.ibm.aspen.core.network.NetworkCodec
import java.nio.ByteBuffer
import com.ibm.aspen.base.FinalizationActionHandler
import com.ibm.aspen.base.FinalizationAction
import scala.concurrent.ExecutionContext
import scala.concurrent.Future
import com.ibm.aspen.base.RetryStrategy
import com.ibm.aspen.base.AspenSystem
import com.ibm.aspen.base.kvtree.KVTreeSimpleFactory
import com.ibm.aspen.base.kvtree.KVTreeNodeCache
import com.ibm.aspen.base.kvtree.KVTree

class AllocationFinalizationAction(
    val retryStrategy: RetryStrategy,
    val system: AspenSystem) extends FinalizationActionHandler {
  
  import AllocationFinalizationAction._
  
  val supportedUUIDs: Set[UUID] = AllocationFinalizationAction.supportedUUIDs
  
  class AddToAllocationTree(
      val storagePoolDefinitionPointer:ObjectPointer, 
      val newNodePointer:ObjectPointer) extends FinalizationAction {
    
    def execute()(implicit ec: ExecutionContext): Future[Unit] = retryStrategy.retryUntilSuccessful {
      //
      // TODO: getStoragePool will forever fail if the pool description object is deleted (old Tx could be recovered after pool is deleted)
      //       detect this condition and return success to retryUntilSuccessful
      //
      system.getStoragePool(storagePoolDefinitionPointer) flatMap {
        pool =>
          implicit val tx = system.newTransaction()
          
          // TODO: Need an intelligent KVTreeFactory here
          val treeFactory = new KVTreeSimpleFactory(system, new UUID(0,0), pool.uuid, pool.poolDefinitionPointer.ida, 
                                                    64*1024, new KVTreeNodeCache {}, KVTree.KeyComparison.Raw)
          for {
            treeDefPointer <- pool.getAllocationTreeDefinitionPointer(retryStrategy)
            tree <- treeFactory.createTree(treeDefPointer)
            commitReady <- tree.put(newNodePointer.uuidAsByteArray, NetworkCodec.objectPointerToByteArray(newNodePointer))
            result <- tx.commit()
          } yield ()
      } 
    }
  
    def completionDetected(): Unit = ()
  }
  
  def createAction(
      finalizationActionUUID: UUID, 
      serializedActionData: Array[Byte]): Option[FinalizationAction] = finalizationActionUUID match {
    
    case AddToAllocationTreeUUID => 
      val fa = decode(serializedActionData)
      
      Some(new AddToAllocationTree(fa.storagePoolDefinitionPointer, fa.newNodePointer))
      
    case _ => None
  }
}

object AllocationFinalizationAction {
  val AddToAllocationTreeUUID = UUID.fromString("909ce37d-a138-44a5-9498-f56095827cdf")
  
  val supportedUUIDs: Set[UUID] = Set(AddToAllocationTreeUUID)
  
  case class FAContent(storagePoolDefinitionPointer:ObjectPointer, newNodePointer:ObjectPointer)
  
  def addToAllocationTree(transaction: Transaction, storagePoolDefinitionPointer:ObjectPointer, newNodePointer:ObjectPointer): Unit = {
    val serializedContent = encode(storagePoolDefinitionPointer, newNodePointer)
    transaction.addFinalizationAction(AddToAllocationTreeUUID, serializedContent)
  }
  
  def encode(storagePoolPointer:ObjectPointer, newNodePointer:ObjectPointer): Array[Byte] = {
     
    val builder = new FlatBufferBuilder(1024)
    
    val storagePoolDefinitionPointerOffset = NetworkCodec.encode(builder, storagePoolPointer)
    val newObjectPointerOffset = NetworkCodec.encode(builder, newNodePointer)
    
    P.AllocationFinalizationActionContent.startAllocationFinalizationActionContent(builder)
    P.AllocationFinalizationActionContent.addStoragePoolDefinitionPointer(builder, storagePoolDefinitionPointerOffset)
    P.AllocationFinalizationActionContent.addNewObjectPointer(builder, newObjectPointerOffset)

    val finalOffset = P.AllocationFinalizationActionContent.endAllocationFinalizationActionContent(builder)
    
    builder.finish(finalOffset)
    
    NetworkCodec.byteBufferToArray(builder.dataBuffer())
  }
 
  def decode(arr: Array[Byte]): FAContent = {
    val n = P.AllocationFinalizationActionContent.getRootAsAllocationFinalizationActionContent(ByteBuffer.wrap(arr))
    val sp = NetworkCodec.decode(n.storagePoolDefinitionPointer())
    val np = NetworkCodec.decode(n.newObjectPointer())
    FAContent(sp, np)
  }
}