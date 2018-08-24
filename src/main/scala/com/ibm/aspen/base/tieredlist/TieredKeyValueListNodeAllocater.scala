package com.ibm.aspen.base.tieredlist

import com.ibm.aspen.base.ObjectAllocater
import scala.concurrent.ExecutionContext
import scala.concurrent.Future
import java.util.UUID
import com.ibm.aspen.core.DataBuffer


trait TieredKeyValueListNodeAllocater {
  val typeUUID: UUID
  
  def tierNodeSizeLimit(tier: Int): Int
    
  def tierNodeKVPairLimit(tier: Int): Int 
  
  def tierNodeAllocater(tier: Int)(implicit ec: ExecutionContext): Future[ObjectAllocater]
  
  def config: DataBuffer
}