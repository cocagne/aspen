package com.ibm.aspen.core.transaction.paxos

case class ProposalID(number: Int, peer: Byte) extends Ordered[ProposalID] {
  def nextProposal = ProposalID(number+1, peer)
  
  def compare(rhs: ProposalID): Int = if (number == rhs.number) 
      peer - rhs.peer
    else
      number - rhs.number
}

object ProposalID {
  def initialProposal(peer: Byte) = ProposalID(1, peer)
}