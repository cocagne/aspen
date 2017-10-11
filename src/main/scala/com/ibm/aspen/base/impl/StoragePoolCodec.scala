package com.ibm.aspen.base.impl

import com.ibm.aspen.base.impl.{codec => P}
import com.google.flatbuffers.FlatBufferBuilder
import java.util.UUID
import com.ibm.aspen.core.network.StorageNodeID
import com.ibm.aspen.core.objects.ObjectPointer
import com.ibm.aspen.core.network.NetworkCodec
import java.nio.ByteBuffer

object StoragePoolCodec {
 
 def encode(
     poolUUID: UUID, 
     hostingStorageNodes: Array[StorageNodeID], 
     allocationTreeDefinition: Option[ObjectPointer]): ByteBuffer = {
    
    val builder = new FlatBufferBuilder(2048)
    
    val d = encode(builder, poolUUID, hostingStorageNodes, allocationTreeDefinition)

    builder.finish(d)
    
    builder.dataBuffer().asReadOnlyBuffer()
  }
 
 def decode(buf: ByteBuffer): (UUID, Array[StorageNodeID], Option[ObjectPointer]) = {
   decode(P.StoragePoolDefinition.getRootAsStoragePoolDefinition(buf.asReadOnlyBuffer()))
 }
 
 def encode(
     builder: FlatBufferBuilder, 
     poolUUID: UUID, 
     hostingStorageNodes: Array[StorageNodeID], 
     allocationTreeDefinition: Option[ObjectPointer]): Int = {
   
   val treeDefOffset = allocationTreeDefinition match {
     case Some(atd) => NetworkCodec.encode(builder, atd)
     case None => -1
   }
   
   P.StoragePoolDefinition.startStoragePoolDefinition(builder)
   P.StoragePoolDefinition.addPoolUUID(builder, NetworkCodec.encode(builder, poolUUID))
   if (treeDefOffset >= 0) P.StoragePoolDefinition.addAllocationTreeDefinition(builder, treeDefOffset)
   P.StoragePoolDefinition.startStoreHostsVector(builder, hostingStorageNodes.length)
   hostingStorageNodes.foreach(snid => NetworkCodec.encode(builder, snid.uuid))
   P.StoragePoolDefinition.endStoragePoolDefinition(builder)
 }
 def decode(n: P.StoragePoolDefinition): (UUID, Array[StorageNodeID], Option[ObjectPointer]) = {
   val poolUUID = NetworkCodec.decode(n.poolUUID())
   
   val atd = n.allocationTreeDefinition()
 
   val treeDefPtr =  if (atd == null) None else Some(NetworkCodec.decode(atd))
   
   val hostingNodes = new Array[StorageNodeID](n.storeHostsLength())
   for (i <- 0 until n.storeHostsLength()) {
     hostingNodes(i) = new StorageNodeID(NetworkCodec.decode(n.storeHosts(i)))
   }
   (poolUUID, hostingNodes, treeDefPtr)
 }
}