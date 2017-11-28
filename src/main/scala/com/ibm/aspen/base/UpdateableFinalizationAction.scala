package com.ibm.aspen.base

import com.ibm.aspen.core.data_store.DataStoreID

/** Mixin class for FinalizationActions that should be updated with the set of peers known
 *  to have successfully processed the transaction. This method will be called each time an
 *  Accepted message is received from a new peer.
 */
trait UpdateableFinalizationAction extends FinalizationActionHandler {
  def updateAcceptedPeers(acceptedPeers: Set[DataStoreID]): Unit 
}