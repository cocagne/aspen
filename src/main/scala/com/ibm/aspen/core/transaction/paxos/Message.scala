package com.ibm.aspen.core.transaction.paxos

sealed abstract class Message

case class Prepare(
    proposalId: ProposalID) extends Message 

case class Nack(
    fromPeer:Byte, 
    proposalId: ProposalID, 
    promisedProposalId: ProposalID) extends Message
    
case class Promise(
    fromPeer: Byte,
    proposalId: ProposalID, 
    lastAccepted: Option[(ProposalID, Boolean)]) extends Message
        
case class Accept( 
    proposalId: ProposalID, 
    proposalValue: Boolean) extends Message

case class Accepted(
    fromPeer: Byte, 
    proposalId: ProposalID, 
    proposalValue: Boolean) extends Message        
