package com.ibm.aspen.base.impl

import java.util.UUID
import com.ibm.aspen.base.Transaction
import com.ibm.aspen.core.objects.ObjectPointer
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
import com.ibm.aspen.core.data_store.DataStoreID

object AllocationFinalizationAction {
  val AddToAllocationTreeUUID = UUID.fromString("909ce37d-a138-44a5-9498-f56095827cdf")
  
  case class FAContent(storagePoolDefinitionPointer:ObjectPointer, newNodePointer:ObjectPointer)
  
  def addToAllocationTree(transaction: Transaction, storagePoolDefinitionPointer:ObjectPointer, newNodePointer:ObjectPointer): Unit = {
    val serializedContent = BaseCodec.encode(FAContent(storagePoolDefinitionPointer, newNodePointer))
    transaction.addFinalizationAction(AddToAllocationTreeUUID, serializedContent)
    
    val notifyStores = newNodePointer.storePointers.map(sp => DataStoreID(newNodePointer.poolUUID, sp.poolIndex)).toSet
    transaction.addNotifyOnResolution(notifyStores)
  }
}

class AllocationFinalizationAction(
    val retryStrategy: RetryStrategy,
    val system: AspenSystem) extends FinalizationActionHandler {
  
  import AllocationFinalizationAction._
  
  val finalizationActionUUID: UUID = AddToAllocationTreeUUID
  
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
  
  override def createAction(serializedActionData: Array[Byte]): FinalizationAction = {

    val fa = BaseCodec.decodeFinalizationActionContent(serializedActionData)
    
    new AddToAllocationTree(fa.storagePoolDefinitionPointer, fa.newNodePointer)      
  }
}

