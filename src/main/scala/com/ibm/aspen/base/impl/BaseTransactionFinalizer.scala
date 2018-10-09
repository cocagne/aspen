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

class BaseTransactionFinalizer(val system: BasicAspenSystem)(implicit ec: ExecutionContext) {
  
  object factory extends TransactionFinalizer.Factory{
    def create(
        txd: TransactionDescription, 
        messenger: StoreSideTransactionMessenger): TransactionFinalizer = new TxFinalizer(txd, messenger)
  }
  
  protected class TxFinalizer(
      val txd: TransactionDescription, 
      val messenger: StoreSideTransactionMessenger) extends TransactionFinalizer {
    
    val falist = txd.finalizationActions.foldLeft(List[FinalizationAction]()) { (l, sfa) => 
      system.typeRegistry.getTypeFactory[FinalizationActionHandler](sfa.typeUUID) match {
        case None => 
          // Preceeding code should not allow this to occur
          assert(false, "Unknown Finalizers or deserialization problems must be caught before this point") 
          l
        case Some(fah) => fah.createAction(system, txd, sfa.data) :: l
      }
    }
    
    val complete: Future[Unit] = Future.sequence(falist.map(_.complete)).map(_=>())

    def debugStatus: List[(String, Boolean)] = falist.map(fa => fa.getClass.getName -> fa.complete.isCompleted)

    def updateCommittedPeer(peer: DataStoreID): Unit = falist.foreach {
      case ufah: UpdateableFinalizationAction => ufah.updateCommittedPeer(peer)
      case _ =>
    }
    
    /** Called when a TxFinalized message is received. */ 
    def cancel(): Unit = falist.foreach(_.completionDetected())
    
  }
}