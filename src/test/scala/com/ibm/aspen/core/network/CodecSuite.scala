package com.ibm.aspen.core.network

import org.scalatest._
import com.google.flatbuffers.FlatBufferBuilder

import com.ibm.aspen.core.network.{protocol => P}
import com.ibm.aspen.core.objects._
import com.ibm.aspen.core.ida._
import com.ibm.aspen.core.transaction._
import com.ibm.aspen.core.data_store.DataStoreID
import com.ibm.aspen.core.transaction.paxos.ProposalID
import com.ibm.aspen.core.allocation.Allocate
import com.ibm.aspen.core.allocation.AllocateResponse
import com.ibm.aspen.core.allocation.AllocationErrors
import com.ibm.aspen.core.read.Read
import com.ibm.aspen.core.read.ReadResponse
import com.ibm.aspen.core.read.ReadError
import java.nio.ByteBuffer
import java.util.UUID
import com.ibm.aspen.core.DataBuffer
import com.ibm.aspen.core.allocation.AllocationRecoveryState
import com.ibm.aspen.core.HLCTimestamp

object CodecSuite {
  
  val (txd, txd2) = {
    val objUuid = java.util.UUID.randomUUID()
    val poolUUID = java.util.UUID.randomUUID()
    val cliUUID = new UUID(1,1)
    val size = 10
    val p1 = StorePointer(0, Array[Byte](0, 1, 2, 3))
    val p2 = StorePointer(1, Array[Byte](3, 2, 1, 0))
    val pointers = (p1::p2::Nil).toArray
    val ida = Replication(3,2)
    
    val op = DataObjectPointer(objUuid, poolUUID, Some(size), ida, pointers)
    
    val txuuid = java.util.UUID.randomUUID()
    val startTs = 100
    val leader: Byte = 4
    val dataUpdates = DataUpdate(op, ObjectRevision(new UUID(0,1)), DataUpdateOperation.Overwrite) :: Nil
    val refcountUpdates = RefcountUpdate(op, ObjectRefcount(1,150), ObjectRefcount(2,150)) :: Nil
    val finalz = SerializedFinalizationAction(java.util.UUID.randomUUID(), Array[Byte](3,4)) :: Nil
    val client = ClientID(cliUUID)
    
    (TransactionDescription(txuuid, startTs, op, leader, dataUpdates, finalz),
        TransactionDescription(java.util.UUID.randomUUID(), startTs, op, leader, refcountUpdates, finalz, Some(client)))
        
  }
}

class CodecSuite extends FunSuite with Matchers {
  
  import CodecSuite._
  /*final case class ReadResponse(
    fromStore: DataStoreID,
    readUUID: UUID,
    result: Either[ReadError.Value, ReadResponse.CurrentState]) extends Message
    
object ReadResponse {
  case class CurrentState(
      revision: ObjectRevision,
      refcount: ObjectRefcount,
      objectData: Option[Array[Byte]],
      lockedTransaction: Option[TransactionDescription])*/
  
  test("Allocate Encoding with read error") {
    val poolUUID = new java.util.UUID(1,2)
    val storeId = DataStoreID(poolUUID, 3)
    val readUUID = new java.util.UUID(3,4)
    
    val rr = ReadResponse(storeId, readUUID, Left(ReadError.ObjectMismatch))
    
    val builder = new FlatBufferBuilder(1024)
    
    val o = NetworkCodec.encode(builder, rr)
    
    P.Message.startMessage(builder)
    P.Message.addReadResponse(builder, o)
    
    val m =  P.Message.endMessage(builder)
    builder.finish(m)
    
    val buf = builder.dataBuffer()
    
    val m2 = P.Message.getRootAsMessage(buf)
    val decoded = NetworkCodec.decode(m2.readResponse())
    
    decoded should be (rr)
  }
  test("Allocate Encoding with data and locked tx") {
    val poolUUID = new java.util.UUID(1,2)
    val storeId = DataStoreID(poolUUID, 3)
    val readUUID = new java.util.UUID(3,4)
    val ref = ObjectRefcount(1,1)
    val rev = ObjectRevision(new UUID(0,2))
    val ts = HLCTimestamp.now
    val cs = ReadResponse.CurrentState(rev, ref, ts, Some(DataBuffer(List[Byte](1,2,3).toArray)), Some(txd))
    
    val rr = ReadResponse(storeId, readUUID, Right(cs))
    
    val builder = new FlatBufferBuilder(1024)
    
    val o = NetworkCodec.encode(builder, rr)
    
    P.Message.startMessage(builder)
    P.Message.addReadResponse(builder, o)
    
    val m =  P.Message.endMessage(builder)
    builder.finish(m)
    
    val buf = builder.dataBuffer()
    
    val m2 = P.Message.getRootAsMessage(buf)
    val decoded = NetworkCodec.decode(m2.readResponse())
    
    decoded should be (rr)
  }
  test("Allocate Encoding without data or locked tx") {
    val poolUUID = new java.util.UUID(1,2)
    val storeId = DataStoreID(poolUUID, 3)
    val readUUID = new java.util.UUID(3,4)
    val ref = ObjectRefcount(1,1)
    val rev = ObjectRevision(new UUID(0, 2))
    val ts = HLCTimestamp.now
    val cs = ReadResponse.CurrentState(rev, ref, ts, None, None)
    
    val rr = ReadResponse(storeId, readUUID, Right(cs))
    
    val builder = new FlatBufferBuilder(1024)
    
    val o = NetworkCodec.encode(builder, rr)
    
    P.Message.startMessage(builder)
    P.Message.addReadResponse(builder, o)
    
    val m =  P.Message.endMessage(builder)
    builder.finish(m)
    
    val buf = builder.dataBuffer()
    
    val m2 = P.Message.getRootAsMessage(buf)
    val decoded = NetworkCodec.decode(m2.readResponse())
    
    decoded should be (rr)
  }
  
  test("Direct ObjectPointer encode/decode") {
    
    val poolUUID = new java.util.UUID(3,4)
    val objUUID = new java.util.UUID(5,6)
    val op = DataObjectPointer(objUUID, poolUUID, None, Replication(3,2), new Array[StorePointer](0))
    
    val bb = NetworkCodec.objectPointerToByteBuffer(op)
    
    bb.position(0)
    
    NetworkCodec.byteBufferToObjectPointer(bb) should be (op)
  }
  
  test("Read Encoding") {
    val c = ClientID(new UUID(1,1))
    val readUUID = new java.util.UUID(1,2)
    val poolUUID = new java.util.UUID(3,4)
    val objUUID = new java.util.UUID(5,6)
    val op = DataObjectPointer(objUUID, poolUUID, None, Replication(3,2), new Array[StorePointer](0))
    val storeId = DataStoreID(poolUUID, 3)
    
    val r = Read(storeId, c, readUUID, op, true, false)
    
    val builder = new FlatBufferBuilder(1024)
    
    val o = NetworkCodec.encode(builder, r)
    
    P.Message.startMessage(builder)
    P.Message.addRead(builder, o)
    
    val m =  P.Message.endMessage(builder)
    builder.finish(m)
    
    val buf = builder.dataBuffer()
    
    val m2 = P.Message.getRootAsMessage(buf)
    val decoded = NetworkCodec.decode(m2.read())
    
    decoded should be (r)
  }
  
  test("AllocateResponse Encoding Error") {
    val poolUUID = new java.util.UUID(1,2)
    val storeId = DataStoreID(poolUUID, 3)
    val txUUID = new java.util.UUID(3,4)
    
    val ar = AllocateResponse(storeId, txUUID, Left(AllocationErrors.InsufficientSpace))
    
    val builder = new FlatBufferBuilder(1024)
    
    val o = NetworkCodec.encode(builder, ar)
    
    P.Message.startMessage(builder)
    P.Message.addAllocateResponse(builder, o)
    
    val m =  P.Message.endMessage(builder)
    builder.finish(m)
    
    val buf = builder.dataBuffer()
    
    val m2 = P.Message.getRootAsMessage(buf)
    val decoded = NetworkCodec.decode(m2.allocateResponse())
    
    decoded should be (ar)
  }
  
  test("AllocateResponse Encoding Success") {
    val poolUUID = new java.util.UUID(1,2)
    val storeId = DataStoreID(poolUUID, 3)
    val txUUID = new java.util.UUID(3,4)
    val objUUID = new java.util.UUID(5,6)
    
    val sp = StorePointer(0, Array[Byte](0, 1, 2, 3))
    val la = List(AllocateResponse.Allocated(objUUID, sp))
    val ar = AllocateResponse(storeId, txUUID, Right(la))
    
    val builder = new FlatBufferBuilder(1024)
    
    val o = NetworkCodec.encode(builder, ar)
    
    P.Message.startMessage(builder)
    P.Message.addAllocateResponse(builder, o)
    
    val m =  P.Message.endMessage(builder)
    builder.finish(m)
    
    val buf = builder.dataBuffer()
    
    val m2 = P.Message.getRootAsMessage(buf)
    val decoded = NetworkCodec.decode(m2.allocateResponse())
    
    decoded should be (ar)
  }
  
  test("Allocate Encoding without Size") {
    val d1 = DataBuffer(List[Byte](1,2,3).toArray)
    val s1:Option[Int] = None
    val c1 = ClientID(new UUID(1,1))
    val ref = ObjectRefcount(1,1)
    val rev = ObjectRevision(new UUID(0,2))
    val poolUUID = new java.util.UUID(1,2)
    val txUUID = new java.util.UUID(3,4)
    val objUUID = new java.util.UUID(5,6)
    val op = DataObjectPointer(objUUID, poolUUID, None, Replication(3,2), new Array[StorePointer](0))
    val storeId = DataStoreID(poolUUID, 3)
    val lno = List(Allocate.NewObject(objUUID, s1, ref, d1))
    val ts = HLCTimestamp.now
    val a1 = Allocate(storeId, c1, lno, ts, txUUID,  op, rev)
    
    val builder = new FlatBufferBuilder(1024)
    
    val o = NetworkCodec.encode(builder, a1)
    
    P.Message.startMessage(builder)
    P.Message.addAllocate(builder, o)
    
    val m =  P.Message.endMessage(builder)
    builder.finish(m)
    
    val buf = builder.dataBuffer()
    
    val m2 = P.Message.getRootAsMessage(buf)
    val decoded = NetworkCodec.decode(m2.allocate())
    
    decoded should be (a1)
  }
  
  test("Allocate Encoding with Size") {
    val d1 = DataBuffer(List[Byte](1,2,3).toArray)
    val s1:Option[Int] = Some(5)
    val c1 = ClientID(new UUID(1,1))
    val ref = ObjectRefcount(1,1)
    val rev = ObjectRevision(new UUID(0, 2))
    val poolUUID = new java.util.UUID(1,2)
    val txUUID = new java.util.UUID(3,4)
    val objUUID = new java.util.UUID(5,6)
    val op = DataObjectPointer(objUUID, poolUUID, None, Replication(3,2), new Array[StorePointer](0))
    val storeId = DataStoreID(poolUUID, 3)
    val lno = List(Allocate.NewObject(objUUID, s1, ref, d1))
    val ts = HLCTimestamp.now
    val a1 = Allocate(storeId, c1, lno, ts, txUUID,  op, rev)
    
    val builder = new FlatBufferBuilder(1024)
    
    val o = NetworkCodec.encode(builder, a1)
    
    P.Message.startMessage(builder)
    P.Message.addAllocate(builder, o)
    
    val m =  P.Message.endMessage(builder)
    builder.finish(m)
    
    val buf = builder.dataBuffer()
    
    val m2 = P.Message.getRootAsMessage(buf)
    val decoded = NetworkCodec.decode(m2.allocate())
    
    decoded should be (a1)
  }
  
  test("TxPrepare Encoding") {
    
    val to = DataStoreID(txd.primaryObject.poolUUID, 2)
    val ds = DataStoreID(txd.primaryObject.poolUUID, 3)
    val pid = ProposalID(4, 3)
    
    val prep = TxPrepare(to, ds, txd, pid)
    
	  val builder = new FlatBufferBuilder(1024)
    
    val o = NetworkCodec.encode(builder, prep)
    
    P.Message.startMessage(builder)
    P.Message.addPrepare(builder, o)
    
    val m =  P.Message.endMessage(builder)
    builder.finish(m)
    
    val buf = builder.dataBuffer()
    
    val m2 = P.Message.getRootAsMessage(buf)
    val prep2 = NetworkCodec.decode(m2.prepare())
    
    prep2 should be(prep)
	}
  
  test("TxPrepareResponse Encoding") {
    
    val to = DataStoreID(txd.primaryObject.poolUUID, 2)
    val ds = DataStoreID(txd.primaryObject.poolUUID, 3)
    val pid = ProposalID(4, 3)
    val pid2 = ProposalID(1, 0)
    val uuid1 = new UUID(1,1)
    val uuid2 = new UUID(2,2)
    
    val e1 = UpdateErrorResponse(uuid1, UpdateError.Collision, Some(ObjectRevision(new UUID(0,1))), Some(ObjectRefcount(1,1)), Some(txd2))
    val e2 = UpdateErrorResponse(uuid2, UpdateError.Collision, None, None, None)
    
    val prep = TxPrepareResponse(
        to,
        ds, 
        txd.transactionUUID, 
        Right(TxPrepareResponse.Promise(Some((pid2, false)))),
        pid,
        TransactionDisposition.VoteAbort,
        e1 :: e2 :: Nil)
    
	  val builder = new FlatBufferBuilder(1024)
    
    val o = NetworkCodec.encode(builder, prep)
    
    P.Message.startMessage(builder)
    P.Message.addPrepareResponse(builder, o)
    
    val m =  P.Message.endMessage(builder)
    builder.finish(m)
    
    val buf = builder.dataBuffer()
    
    val m2 = P.Message.getRootAsMessage(buf)
    val prep2 = NetworkCodec.decode(m2.prepareResponse())
    
    prep2 should be(prep)
	}
  
  test("TxPrepareResponse Encoding2") {
    
    val to = DataStoreID(txd.primaryObject.poolUUID, 2)
    val ds = DataStoreID(txd.primaryObject.poolUUID, 3)
    val pid = ProposalID(4, 3)
    val pid2 = ProposalID(1, 0)
    
    val prep = TxPrepareResponse(
        to,
        ds, 
        txd.transactionUUID, 
        Left(TxPrepareResponse.Nack(pid2)),
        pid,
        TransactionDisposition.VoteAbort,
        Nil)
    
	  val builder = new FlatBufferBuilder(1024)
    
    val o = NetworkCodec.encode(builder, prep)
    
    P.Message.startMessage(builder)
    P.Message.addPrepareResponse(builder, o)
    
    val m =  P.Message.endMessage(builder)
    builder.finish(m)
    
    val buf = builder.dataBuffer()
    
    val m2 = P.Message.getRootAsMessage(buf)
    val prep2 = NetworkCodec.decode(m2.prepareResponse())
    
    prep2 should be(prep)
	}

  test("TxAccept Encoding") {
    
    val to = DataStoreID(txd.primaryObject.poolUUID, 2)
    val ds = DataStoreID(txd.primaryObject.poolUUID, 3)
    val pid = ProposalID(4, 3)
    
    val a = TxAccept(to, ds, txd.transactionUUID, pid, true)
    
	  val builder = new FlatBufferBuilder(1024)
    
    val o = NetworkCodec.encode(builder, a)
    
    P.Message.startMessage(builder)
    P.Message.addAccept(builder, o)
    
    val m =  P.Message.endMessage(builder)
    builder.finish(m)
    
    val buf = builder.dataBuffer()
    
    val m2 = P.Message.getRootAsMessage(buf)
    val a2 = NetworkCodec.decode(m2.accept())
    
    a2 should be(a)
	}
  
  test("TxAccepted Encoding") {
    
    val to = DataStoreID(txd.primaryObject.poolUUID, 2)
    val ds = DataStoreID(txd.primaryObject.poolUUID, 3)
    val pid = ProposalID(4, 3)
    
    val a = TxAcceptResponse(to, ds, txd.transactionUUID, pid, Left(TxAcceptResponse.Nack(pid)))
    
	  val builder = new FlatBufferBuilder(1024)
    
    val o = NetworkCodec.encode(builder, a)
    
    P.Message.startMessage(builder)
    P.Message.addAcceptResponse(builder, o)
    
    val m =  P.Message.endMessage(builder)
    builder.finish(m)
    
    val buf = builder.dataBuffer()
    
    val m2 = P.Message.getRootAsMessage(buf)
    val a2 = NetworkCodec.decode(m2.acceptResponse())
    
    a2 should be(a)
	}
  
  test("TxFinalized Encoding") {
    
    val to = DataStoreID(txd.primaryObject.poolUUID, 2)
    val ds = DataStoreID(txd.primaryObject.poolUUID, 3)
    val pid = ProposalID(4, 3)
    
    val a = TxFinalized(to, ds, txd.transactionUUID, true)
    
	  val builder = new FlatBufferBuilder(1024)
    
    val o = NetworkCodec.encode(builder, a)
    
    P.Message.startMessage(builder)
    P.Message.addFinalized(builder, o)
    
    val m =  P.Message.endMessage(builder)
    builder.finish(m)
    
    val buf = builder.dataBuffer()
    
    val m2 = P.Message.getRootAsMessage(buf)
    val a2 = NetworkCodec.decode(m2.finalized())
    
    a2 should be(a)
	}
}