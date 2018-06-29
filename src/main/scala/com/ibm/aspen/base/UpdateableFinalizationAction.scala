package com.ibm.aspen.base

import com.ibm.aspen.core.data_store.DataStoreID

/** Mixin class for FinalizationActions that should be updated with the peers known to have
 *  committed the transaction. Multiple calls for the same peer my occur.
 */
trait UpdateableFinalizationAction extends FinalizationAction {
  def updateCommittedPeer(peer: DataStoreID): Unit 
}