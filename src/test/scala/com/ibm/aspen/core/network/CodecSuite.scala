package com.ibm.aspen.core.network

import org.scalatest._
import com.google.flatbuffers.FlatBufferBuilder

import com.ibm.aspen.core.network.{protocol => P}
import com.ibm.aspen.core.objects._
import com.ibm.aspen.core.ida._
import com.ibm.aspen.core.transaction._
import com.ibm.aspen.core.data_store.DataStoreID
import com.ibm.aspen.core.transaction.paxos.ProposalID

object CodecSuite {
  
  val (txd, txd2) = {
    val objUuid = java.util.UUID.randomUUID()
    val poolUUID = java.util.UUID.randomUUID()
    val size = 10
    val p1 = StorePointer(0, Array[Byte](0, 1, 2, 3))
    val p2 = StorePointer(1, Array[Byte](3, 2, 1, 0))
    val pointers = (p1::p2::Nil).toArray
    val ida = Replication(3,2)
    
    val op = ObjectPointer(objUuid, poolUUID, Some(size), ida, pointers)
    
    val txuuid = java.util.UUID.randomUUID()
    val startTs = 100
    val leader: Byte = 4
    val dataUpdates = DataUpdate(op, ObjectRevision(1,150), DataUpdateOperation.Overwrite) :: Nil
    val refcountUpdates = RefcountUpdate(op, ObjectRefcount(1,150), ObjectRefcount(2,150)) :: Nil
    val finalz = SerializedFinalizationAction(java.util.UUID.randomUUID(), Array[Byte](3,4)) :: Nil
    
    (TransactionDescription(txuuid, startTs, op, leader, dataUpdates, Nil, finalz),
        TransactionDescription(java.util.UUID.randomUUID(), startTs, op, leader, Nil, refcountUpdates, finalz))
        
  }
}

class CodecSuite extends FunSuite with Matchers {
  
  import CodecSuite._
  
  test("TxPrepare Encoding") {
    
    val ds = DataStoreID(txd.primaryObject.poolUUID, 3)
    val pid = ProposalID(4, 3)
    
    val prep = TxPrepare(ds, txd, pid)
    
	  val builder = new FlatBufferBuilder(1024)
    
    val o = Codec.encode(builder, prep)
    
    P.Message.startMessage(builder)
    P.Message.addPrepare(builder, o)
    
    val m =  P.Message.endMessage(builder)
    builder.finish(m)
    
    val buf = builder.dataBuffer()
    
    val m2 = P.Message.getRootAsMessage(buf)
    val prep2 = Codec.decode(m2.prepare())
    
    prep2 should be(prep)
	}
  
  test("TxPrepareResponse Encoding") {
    
    val ds = DataStoreID(txd.primaryObject.poolUUID, 3)
    val pid = ProposalID(4, 3)
    val pid2 = ProposalID(1, 0)
    
    val e1 = UpdateErrorResponse(UpdateType.Data, 0, UpdateError.Collision, Some(ObjectRevision(1,150)), Some(ObjectRefcount(1,1)), Some(txd2))
    val e2 = UpdateErrorResponse(UpdateType.Refcount, 0, UpdateError.Collision, None, None, None)
    
    val prep = TxPrepareResponse(
        ds, 
        txd.transactionUUID, 
        Right(TxPrepareResponse.Promise(Some((pid2, false)))),
        pid,
        TransactionDisposition.VoteAbort,
        e1 :: e2 :: Nil)
    
	  val builder = new FlatBufferBuilder(1024)
    
    val o = Codec.encode(builder, prep)
    
    P.Message.startMessage(builder)
    P.Message.addPrepareResponse(builder, o)
    
    val m =  P.Message.endMessage(builder)
    builder.finish(m)
    
    val buf = builder.dataBuffer()
    
    val m2 = P.Message.getRootAsMessage(buf)
    val prep2 = Codec.decode(m2.prepareResponse())
    
    prep2 should be(prep)
	}
  
  test("TxPrepareResponse Encoding2") {
    
    val ds = DataStoreID(txd.primaryObject.poolUUID, 3)
    val pid = ProposalID(4, 3)
    val pid2 = ProposalID(1, 0)
    
    val prep = TxPrepareResponse(
        ds, 
        txd.transactionUUID, 
        Left(TxPrepareResponse.Nack(pid2)),
        pid,
        TransactionDisposition.VoteAbort,
        Nil)
    
	  val builder = new FlatBufferBuilder(1024)
    
    val o = Codec.encode(builder, prep)
    
    P.Message.startMessage(builder)
    P.Message.addPrepareResponse(builder, o)
    
    val m =  P.Message.endMessage(builder)
    builder.finish(m)
    
    val buf = builder.dataBuffer()
    
    val m2 = P.Message.getRootAsMessage(buf)
    val prep2 = Codec.decode(m2.prepareResponse())
    
    prep2 should be(prep)
	}

  test("TxAccept Encoding") {
    
    val ds = DataStoreID(txd.primaryObject.poolUUID, 3)
    val pid = ProposalID(4, 3)
    
    val a = TxAccept(ds, txd.transactionUUID, pid, true)
    
	  val builder = new FlatBufferBuilder(1024)
    
    val o = Codec.encode(builder, a)
    
    P.Message.startMessage(builder)
    P.Message.addAccept(builder, o)
    
    val m =  P.Message.endMessage(builder)
    builder.finish(m)
    
    val buf = builder.dataBuffer()
    
    val m2 = P.Message.getRootAsMessage(buf)
    val a2 = Codec.decode(m2.accept())
    
    a2 should be(a)
	}
  
  test("TxAccepted Encoding") {
    
    val ds = DataStoreID(txd.primaryObject.poolUUID, 3)
    val pid = ProposalID(4, 3)
    
    val a = TxAcceptResponse(ds, txd.transactionUUID, pid, Left(TxAcceptResponse.Nack(pid)))
    
	  val builder = new FlatBufferBuilder(1024)
    
    val o = Codec.encode(builder, a)
    
    P.Message.startMessage(builder)
    P.Message.addAcceptResponse(builder, o)
    
    val m =  P.Message.endMessage(builder)
    builder.finish(m)
    
    val buf = builder.dataBuffer()
    
    val m2 = P.Message.getRootAsMessage(buf)
    val a2 = Codec.decode(m2.acceptResponse())
    
    a2 should be(a)
	}
  
  test("TxFinalized Encoding") {
    
    val ds = DataStoreID(txd.primaryObject.poolUUID, 3)
    val pid = ProposalID(4, 3)
    
    val a = TxFinalized(ds, txd.transactionUUID, true)
    
	  val builder = new FlatBufferBuilder(1024)
    
    val o = Codec.encode(builder, a)
    
    P.Message.startMessage(builder)
    P.Message.addFinalized(builder, o)
    
    val m =  P.Message.endMessage(builder)
    builder.finish(m)
    
    val buf = builder.dataBuffer()
    
    val m2 = P.Message.getRootAsMessage(buf)
    val a2 = Codec.decode(m2.finalized())
    
    a2 should be(a)
	}
}