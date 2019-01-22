package com.ibm.aspen.core.transaction

case class TransactionData(localUpdates: List[LocalUpdate],
                           preTransactionRebuilds: List[PreTransactionOpportunisticRebuild])
