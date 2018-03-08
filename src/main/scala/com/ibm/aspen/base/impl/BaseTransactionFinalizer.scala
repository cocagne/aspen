package com.ibm.aspen.base.impl

import com.ibm.aspen.core.transaction.TransactionFinalizer
import scala.concurrent.Future
import com.ibm.aspen.core.data_store.DataStoreID
import com.ibm.aspen.core.transaction.TransactionDescription
import com.ibm.aspen.core.network.StoreSideTransactionMessenger
import com.ibm.aspen.base.FinalizationAction
import scala.concurrent.ExecutionContext
import com.ibm.aspen.base.UpdateableFinalizationAction
import com.ibm.aspen.base.TypeRegistry
import com.ibm.aspen.base.FinalizationActionHandler

class BaseTransactionFinalizer(
    system: BasicAspenSystem,
    registry: TypeRegistry[FinalizationActionHandler])(implicit ec: ExecutionContext) {
  
  object factory extends TransactionFinalizer.Factory{
    def create(
        txd: TransactionDescription, 
        acceptedPeers: Set[DataStoreID], 
        messenger: StoreSideTransactionMessenger): TransactionFinalizer = new TxFinalizer(txd, acceptedPeers, messenger)
  }
  
  protected class TxFinalizer(
      val txd: TransactionDescription, 
      private var acceptedPeers: Set[DataStoreID], 
      val messenger: StoreSideTransactionMessenger) extends TransactionFinalizer {
    
    val falist = txd.finalizationActions.foldLeft(List[FinalizationAction]()) { (l, sfa) => 
      registry.getTypeFactory(sfa.typeUUID) match {
        case None => 
          // Preceeding code should not allow this to occur
          assert(false, "Unknown Finalizers or deserialization problems must be caught before this point") 
          l
        case Some(fah) => fah.createAction(sfa.data) :: l
      }
    }
    
    val complete: Future[Unit] = Future.sequence(falist.map(_.execute())).map(_=>())
    
    def updateAcceptedPeers(acceptedPeers: Set[DataStoreID]): Unit = falist.foreach { fa =>
      fa match {
        case ufah: UpdateableFinalizationAction => ufah.updateAcceptedPeers(acceptedPeers)
        case _ =>
      }
    }
    
    /** Called when a TxFinalized message is received. */ 
    def cancel(): Unit = falist.foreach(_.completionDetected())
    
  }
}