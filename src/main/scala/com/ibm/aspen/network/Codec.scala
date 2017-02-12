package com.ibm.aspen.network

import com.ibm.aspen.network.{protocol => P}
import com.google.flatbuffers.FlatBufferBuilder
import com.ibm.aspen.objects.ObjectRevision
import com.ibm.aspen.objects.ObjectRefcount
import com.ibm.aspen.ida.IDA
import com.ibm.aspen.ida.Replication
import com.ibm.aspen.ida.ReedSolomon
import com.ibm.aspen.objects.StorePointer
import com.ibm.aspen.objects.ObjectPointer
import java.util.UUID



object Codec {
  
  //-----------------------------------------------------------------------------------------------
  // Objects
  //
  def encode(builder:FlatBufferBuilder, u: UUID): Int = {
    P.UUID.createUUID(builder, u.getMostSignificantBits, u.getLeastSignificantBits)
  }
  def decode(o: P.UUID): UUID = {
    new UUID(o.mostSigBits(), o.leastSigBits())
  }
  
  
  def encode(builder:FlatBufferBuilder, rev:ObjectRevision): Int = {
    P.ObjectRevision.createObjectRevision(builder, rev.overwriteCount, rev.currentSize)
  }
  def decode(orev: P.ObjectRevision): ObjectRevision = {
    ObjectRevision(orev.overwriteCount(), orev.currentSize())
  }
  
  
  def encode(builder:FlatBufferBuilder, ref:ObjectRefcount): Int = {
    P.ObjectRefcount.createObjectRefcount(builder, ref.updateSerial, ref.count)
  }
  def decode(oref: P.ObjectRefcount): ObjectRefcount = {
    ObjectRefcount(oref.updateSerial(), oref.refcount())
  }
  
  
  def encode(builder:FlatBufferBuilder, o:StorePointer): Int = {
    val d = P.StorePointer.createDataVector(builder, o.data)
    P.StorePointer.createStorePointer(builder, o.poolIndex, d)
  }
  def decode(n: P.StorePointer): StorePointer = {
    val d = new Array[Byte](n.dataLength())
    n.dataAsByteBuffer().get(d)
    StorePointer(n.storeIndex(), d)
  }
  
  
  def encode(builder:FlatBufferBuilder, o:ObjectPointer): Int = {
    val uuid = encode(builder, o.uuid)
    val poolUUID = encode(builder, o.poolUUID)
    val ida = encode(builder, o.ida)
    val storePointers = P.ObjectPointer.createStorePointersVector(builder, o.storePointers.map(sp => encode(builder, sp)))
    P.ObjectPointer.startObjectPointer(builder)
    P.ObjectPointer.addUuid(builder, uuid)
    P.ObjectPointer.addPoolUuid(builder, poolUUID)
    o.size.foreach(s => P.ObjectPointer.addSize(builder, s))
    P.ObjectPointer.addIda(builder, ida)
    P.ObjectPointer.addStorePointers(builder, storePointers)
    P.ObjectPointer.endObjectPointer(builder)
  }
  def decode(n: P.ObjectPointer): ObjectPointer = {
    val uuid =  decode(n.uuid())
    val poolUUID = decode(n.poolUuid())
    val size = if (n.size() == 0) None else Some(n.size())
    
    val ida = if (n.idaType() == P.IDA.Replication)
      decode(n.ida(new P.Replication).asInstanceOf[P.Replication])
    else
      decode(n.ida(new P.ReedSolomon).asInstanceOf[P.ReedSolomon])
      
    def tolist(idx: Int, lst:List[StorePointer]): List[StorePointer] = {
      if (idx == -1) 
        lst 
      else 
        tolist(idx-1, decode(n.storePointers(idx)) :: lst)
    }
    
    val storePointers = tolist(n.storePointersLength()-1, Nil).toArray
    
    new ObjectPointer(uuid, poolUUID, size, ida, storePointers)
  }
  
  //-----------------------------------------------------------------------------------------------
  // IDA
  //
  def idaType(ida:IDA): Byte = ida match {
    case x: Replication => P.IDA.Replication
    case x: ReedSolomon => P.IDA.ReedSolomon
  }
  def encode(builder:FlatBufferBuilder, ida:IDA): Int = ida match {
    case x: Replication => P.Replication.createReplication(builder, x.width, x.writeThreshold)
    case x: ReedSolomon => P.ReedSolomon.createReedSolomon(builder, x.width, x.restoreThreshold, x.writeThreshold)
  }
  def decode(b:P.Replication): Replication = Replication(b.width(), b.writeThreshold())
  def decode(b:P.ReedSolomon): ReedSolomon = ReedSolomon(b.width(), b.readThreshold(), b.writeThreshold())
    
  
}