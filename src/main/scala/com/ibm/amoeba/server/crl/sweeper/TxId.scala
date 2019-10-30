package com.ibm.amoeba.server.crl.sweeper

import com.ibm.amoeba.common.store.StoreId
import com.ibm.amoeba.common.transaction.TransactionId

final class TxId(val storeId: StoreId, val transactionId: TransactionId) {

  override def equals(other: Any): Boolean = {
    other match {
      case that: TxId => that.storeId == this.storeId && that.transactionId == this.transactionId
      case _ => false
    }
  }

  override def hashCode: Int = (storeId, transactionId).##
}
