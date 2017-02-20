package com.ibm.aspen.core.transaction.paxos

case class PersistentState(
    promised: Option[ProposalID],
    accepted: Option[(ProposalID, Boolean)])