package com.ibm.aspen.base.impl

import com.google.flatbuffers.FlatBufferBuilder
import java.nio.ByteBuffer
import com.ibm.aspen.core.network.NetworkCodec
import com.ibm.aspen.base.impl.{codec => P}
import com.ibm.aspen.core.objects.ObjectPointer
import com.ibm.aspen.base.impl.AllocationFinalizationAction.FAContent
import java.util.UUID
import com.ibm.aspen.core.network.StorageNodeID
import com.ibm.aspen.core.DataBuffer

object BaseCodec {
  
  //-------------------------------------------------------------------------------------------------------------------
  // StoragePoolDefinition
  //
  def encodeStoragePoolDefinition(
     poolUUID: UUID, 
     hostingStorageNodes: Array[StorageNodeID], 
     allocationTreeDefinition: Option[ObjectPointer]): DataBuffer = {
    
    val builder = new FlatBufferBuilder(2048)
    
    val d = encodeStoragePoolDefinitionToOffset(builder, poolUUID, hostingStorageNodes, allocationTreeDefinition)

    builder.finish(d)
    
    DataBuffer(builder.dataBuffer())
  }
 
  def encodeStoragePoolDefinitionToOffset(
      builder: FlatBufferBuilder, 
      poolUUID: UUID, 
      hostingStorageNodes: Array[StorageNodeID], 
      allocationTreeDefinition: Option[ObjectPointer]): Int = {
   
    val treeDefOffset = allocationTreeDefinition match {
      case Some(atd) => NetworkCodec.encode(builder, atd)
      case None => -1
    }
    
    val offsets = new Array[Int](hostingStorageNodes.length)
    
    P.StoragePoolDefinition.startStoreHostsVector(builder, hostingStorageNodes.length)
    
    // Vectors are filled back-to-front
    for (i <- hostingStorageNodes.length -1 to 0 by -1) {
      NetworkCodec.encode(builder, hostingStorageNodes(i).uuid)
    }
    
    val storeHostsOffset = builder.endVector()
    
    P.StoragePoolDefinition.startStoragePoolDefinition(builder)
    P.StoragePoolDefinition.addPoolUUID(builder, NetworkCodec.encode(builder, poolUUID))
    P.StoragePoolDefinition.addStoreHosts(builder, storeHostsOffset)
    if (treeDefOffset >= 0) P.StoragePoolDefinition.addAllocationTreeDefinition(builder, treeDefOffset)
    P.StoragePoolDefinition.endStoragePoolDefinition(builder)
  }
  
  def decodeStoragePoolDefinition(buf: DataBuffer): (UUID, Array[StorageNodeID], Option[ObjectPointer]) = {
    decodeStoragePoolDefinition(P.StoragePoolDefinition.getRootAsStoragePoolDefinition(buf.asReadOnlyBuffer()))
  }
  
  def decodeStoragePoolDefinition(n: P.StoragePoolDefinition): (UUID, Array[StorageNodeID], Option[ObjectPointer]) = {
    val poolUUID = NetworkCodec.decode(n.poolUUID())
    
    val atd = n.allocationTreeDefinition()
  
    val treeDefPtr =  if (atd == null) None else Some(NetworkCodec.decode(atd))
    
    val hostingNodes = new Array[StorageNodeID](n.storeHostsLength())
    for (i <- 0 until n.storeHostsLength()) {
      hostingNodes(i) = new StorageNodeID(NetworkCodec.decode(n.storeHosts(i)))
    }
    (poolUUID, hostingNodes, treeDefPtr)
  }
 
  //-------------------------------------------------------------------------------------------------------------------
  // AllocationFinalizationActionContent
  //
  def encode(fac: FAContent): Array[Byte] = {
     
    val builder = new FlatBufferBuilder(1024)
    
    val storagePoolDefinitionPointerOffset = NetworkCodec.encode(builder, fac.storagePoolDefinitionPointer)
    val newObjectPointerOffset = NetworkCodec.encode(builder, fac.newNodePointer)
    
    P.AllocationFinalizationActionContent.startAllocationFinalizationActionContent(builder)
    P.AllocationFinalizationActionContent.addStoragePoolDefinitionPointer(builder, storagePoolDefinitionPointerOffset)
    P.AllocationFinalizationActionContent.addNewObjectPointer(builder, newObjectPointerOffset)

    val finalOffset = P.AllocationFinalizationActionContent.endAllocationFinalizationActionContent(builder)
    
    builder.finish(finalOffset)
    
    NetworkCodec.byteBufferToArray(builder.dataBuffer())
  }
 
  def decodeFinalizationActionContent(arr: Array[Byte]): FAContent = decode(P.AllocationFinalizationActionContent.getRootAsAllocationFinalizationActionContent(ByteBuffer.wrap(arr))) 
  
  def decode(n: P.AllocationFinalizationActionContent): FAContent = {
    val sp = NetworkCodec.decode(n.storagePoolDefinitionPointer())
    val np = NetworkCodec.decode(n.newObjectPointer())
    FAContent(sp, np)
  }
  
  //-------------------------------------------------------------------------------------------------------------------
  // RadicleContent
  //
  def encode(radicle: Radicle): Array[Byte] = {
     
    val builder = new FlatBufferBuilder(1024)
    
    val systemTreeDefinitionPointerOffset = NetworkCodec.encode(builder, radicle.systemTreeDefinitionPointer)
    
    P.RadicleContent.startRadicleContent(builder)
    P.RadicleContent.addSystemTreeDefinitionPointer(builder, systemTreeDefinitionPointerOffset)

    val finalOffset = P.RadicleContent.endRadicleContent(builder)
    
    builder.finish(finalOffset)
    
    NetworkCodec.byteBufferToArray(builder.dataBuffer())
  }
  
  def decodeRadicle(buf: DataBuffer): Radicle = decode(P.RadicleContent.getRootAsRadicleContent(buf.asReadOnlyBuffer()))
  
  def decode(n: P.RadicleContent): Radicle = {
    val sp = NetworkCodec.decode(n.systemTreeDefinitionPointer())
    Radicle(sp)
  }
  
  
}