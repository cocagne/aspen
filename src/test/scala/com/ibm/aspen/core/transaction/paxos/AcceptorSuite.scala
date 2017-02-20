package com.ibm.aspen.core.transaction.paxos

import org.scalatest._

class AcceptorSuite extends FunSuite with Matchers {
  
  test("Recover state") {
    val istate = PersistentState(Some(ProposalID(1,1)), Some((ProposalID(2,2), false)))
    
	  val a = new Acceptor(0, istate)
    
    a.persistentState should be (istate)
	}
  
  test("Promise first") {
	  val a = new Acceptor(0)
    
    a.receivePrepare(Prepare(ProposalID(1,1))) should be (Right(Promise(0, ProposalID(1,1), None)))
    
    a.persistentState should be (PersistentState(Some(ProposalID(1,1)), None))
	}
  
  test("Promise higher") {
	  val a = new Acceptor(0)
    
    a.receivePrepare(Prepare(ProposalID(1,1))) should be (Right(Promise(0, ProposalID(1,1), None)))
    a.receivePrepare(Prepare(ProposalID(2,2))) should be (Right(Promise(0, ProposalID(2,2), None)))
    a.persistentState should be (PersistentState(Some(ProposalID(2,2)), None))
	}
  
  test("Nack promise lower") {
	  val a = new Acceptor(0)
    
    a.receivePrepare(Prepare(ProposalID(1,1))) should be (Right(Promise(0, ProposalID(1,1), None)))
    a.receivePrepare(Prepare(ProposalID(2,2))) should be (Right(Promise(0, ProposalID(2,2), None)))
    a.persistentState should be (PersistentState(Some(ProposalID(2,2)), None))
	  
	  a.receivePrepare(Prepare(ProposalID(1,0))) should be (Left(Nack(0, ProposalID(1,0), ProposalID(2,2))))
	}
  
  test("Accept first") {
	  val a = new Acceptor(0)
    
	  a.receiveAccept(Accept(ProposalID(1,1), true)) should be (Right(Accepted(0, ProposalID(1,1), true)))
    
    a.persistentState should be (PersistentState(Some(ProposalID(1,1)), Some((ProposalID(1,1),true))))
	}
  
  test("Accept higher") {
	  val a = new Acceptor(0)
    
	  a.receiveAccept(Accept(ProposalID(1,1), true)) should be (Right(Accepted(0, ProposalID(1,1), true)))
    a.receiveAccept(Accept(ProposalID(2,1), false)) should be (Right(Accepted(0, ProposalID(2,1), false)))
    a.persistentState should be (PersistentState(Some(ProposalID(2,1)), Some((ProposalID(2,1),false))))
	}
  
  test("Nack accept lower") {
	  val a = new Acceptor(0)
    
	  a.receiveAccept(Accept(ProposalID(2,1), false)) should be (Right(Accepted(0, ProposalID(2,1), false)))
    a.receiveAccept(Accept(ProposalID(1,0), false)) should be (Left(Nack(0, ProposalID(1,0), ProposalID(2,1))))
	  a.persistentState should be (PersistentState(Some(ProposalID(2,1)), Some((ProposalID(2,1),false))))
	}
  
}