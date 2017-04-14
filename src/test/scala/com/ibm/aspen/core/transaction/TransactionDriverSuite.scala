package com.ibm.aspen.core.transaction

import org.scalatest._
import com.ibm.aspen.core.objects.ObjectPointer
import com.ibm.aspen.core.ida.Replication
import com.ibm.aspen.core.objects.StorePointer
import com.ibm.aspen.core.data_store.DataStoreID
import com.ibm.aspen.core.transaction.paxos.ProposalID
import com.ibm.aspen.core.network.NullMessenger

object TransactionDriverSuite {
  val poolUUID = java.util.UUID.randomUUID()
  
  def mkobj = ObjectPointer(java.util.UUID.randomUUID(), poolUUID, None, Replication(3,2), new Array[StorePointer](0))
  
  def mktxd(du: List[DataUpdate], ru: List[RefcountUpdate]) = TransactionDescription(
      java.util.UUID.randomUUID(), 100, mkobj, 0, du, ru, Nil)
      
  def mkprep(paxosRound: Int, fromPeer: Byte, txd: TransactionDescription) = TxPrepare(DataStoreID(poolUUID,fromPeer), txd, ProposalID(paxosRound,fromPeer))
  
  object HaveContent extends LocalUpdateContent {
    def haveDataForUpdateIndex(updateIndex: Int): Boolean = true
  }
  
  object LackContent extends LocalUpdateContent {
    def haveDataForUpdateIndex(updateIndex: Int): Boolean = false
  }
  
  class TMessenger extends NullMessenger {
    var messages = List[Message]()
    
    override def send(toStore: DataStoreID, message: Message, updateContent: Option[LocalUpdateContent]): Unit = messages = message :: messages
    
    def clear(): Unit = messages = List()
  }
}
class TransactionDriverSuite extends FunSuite with Matchers {
  import TransactionDriverSuite._
}