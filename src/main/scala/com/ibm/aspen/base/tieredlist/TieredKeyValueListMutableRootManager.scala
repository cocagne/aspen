package com.ibm.aspen.base.tieredlist

import java.util.UUID
import com.ibm.aspen.core.objects.KeyValueObjectPointer
import scala.concurrent.Future
import scala.concurrent.ExecutionContext
import com.ibm.aspen.base.Transaction
import com.ibm.aspen.base.AspenSystem

trait TieredKeyValueListMutableRootManager extends TieredKeyValueListRootManager {
  val typeUUID: UUID
  val system: AspenSystem
  
  def serialize(): Array[Byte]
  
  def prepareRootUpdate(
      newRootTier: Int,
      allocater: TieredKeyValueListNodeAllocater,
      inserted: List[KeyValueListPointer])(implicit tx: Transaction, ec: ExecutionContext): Future[Unit]
  
  def prepareRootDeletion()(implicit tx: Transaction, ec: ExecutionContext): Future[Unit]
  
  def getAllocater(): TieredKeyValueListNodeAllocater = {
    val f = system.typeRegistry.getTypeFactory[TieredKeyValueListNodeAllocaterFactory](root.allocaterType).get
    f.createNodeAllocater(system, root.allocaterConfig)
  }
}