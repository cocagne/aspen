package com.ibm.aspen.base.tieredlist

import scala.concurrent.ExecutionContext
import scala.concurrent.Future
import com.ibm.aspen.base.ObjectAllocater
import java.util.UUID
import com.ibm.aspen.base.AspenSystem
import java.nio.ByteBuffer
import com.ibm.aspen.base.TypeFactory
import com.ibm.aspen.core.DataBuffer

class SimpleTieredKeyValueListNodeAllocater(
    val sys: AspenSystem,
    private val tierObjectAllocaters: Array[UUID], 
    private val tierNodeSizes: Array[Int], 
    private val teirNodePairLimit: Array[Int]) extends TieredKeyValueListNodeAllocater {
  
  val typeUUID: UUID = SimpleTieredKeyValueListNodeAllocater.typeUUID
  
  def tierNodeSizeLimit(tier: Int): Int = if (tier < tierNodeSizes.length) tierNodeSizes(tier) else tierNodeSizes.last
    
  def tierNodeKVPairLimit(tier: Int): Int = if (tier < teirNodePairLimit.length) teirNodePairLimit(tier) else teirNodePairLimit.last
  
  def tierNodeAllocater(tier: Int)(implicit ec: ExecutionContext): Future[ObjectAllocater] = {
    val allocaterUUID = if (tier < tierObjectAllocaters.length) tierObjectAllocaters(tier) else tierObjectAllocaters.last
    sys.getObjectAllocater(allocaterUUID)
  }
  
  def serializedSize: Int = SimpleTieredKeyValueListNodeAllocater.encodedSize(tierObjectAllocaters, tierNodeSizes, teirNodePairLimit)
  
  def config: DataBuffer = DataBuffer(SimpleTieredKeyValueListNodeAllocater.encode(tierObjectAllocaters, tierNodeSizes, teirNodePairLimit))
}

object SimpleTieredKeyValueListNodeAllocater extends TieredKeyValueListNodeAllocaterFactory {
  
  val typeUUID: UUID = UUID.fromString("3b4cf4c1-feea-48d7-8278-ad3c8304feaf")
  
  def encodedSize(
      tierObjectAllocaters: Array[UUID], 
      tierNodeSizes: Array[Int], 
      teirNodePairLimit: Array[Int]): Int = {
    4 + tierObjectAllocaters.length * 16 + tierNodeSizes.length * 4 + teirNodePairLimit.length * 4
  }
  
  def encode(tierObjectAllocaters: Array[UUID], tierNodeSizes: Array[Int], teirNodePairLimit: Array[Int]): DataBuffer = {
    val arr = new Array[Byte](encodedSize(tierObjectAllocaters, tierNodeSizes, teirNodePairLimit))
    val bb = ByteBuffer.wrap(arr)
    bb.put(0.asInstanceOf[Byte]) // Placeholder for a version number
    bb.put(tierObjectAllocaters.length.asInstanceOf[Byte])
    bb.put(tierNodeSizes.length.asInstanceOf[Byte])
    bb.put(teirNodePairLimit.length.asInstanceOf[Byte])
    tierObjectAllocaters.foreach { uuid =>
      bb.putLong(uuid.getMostSignificantBits)
      bb.putLong(uuid.getLeastSignificantBits)
    }
    tierNodeSizes.foreach { sz => bb.putInt(sz) }
    teirNodePairLimit.foreach { sz => bb.putInt(sz) }
    
    arr
  }
  
  def createNodeAllocater(sys: AspenSystem, db: DataBuffer): SimpleTieredKeyValueListNodeAllocater = {
    val bb = db.asReadOnlyBuffer()
    
    bb.get() // Placeholder for a version number
    val numAllocaters = bb.get()
    val numSizes = bb.get()
    val numKVLimits = bb.get()
    
    val objectAllocaters = new Array[UUID](numAllocaters)
    for (i <- 0 until numAllocaters) {
      val msb = bb.getLong()
      val lsb = bb.getLong()
      objectAllocaters(i) = new UUID(msb, lsb)
    }
    
    val tierNodeSizes = new Array[Int](numSizes)
    for (i <- 0 until numSizes)
      tierNodeSizes(i) = bb.getInt()
      
    val kvPairLimits = new Array[Int](numKVLimits)
    for (i <- 0 until numKVLimits)
      kvPairLimits(i) = bb.getInt()
    
    new SimpleTieredKeyValueListNodeAllocater(sys, objectAllocaters, tierNodeSizes, kvPairLimits)
  }
}