package com.ibm.aspen.base

import com.ibm.aspen.core.DataBuffer
import com.ibm.aspen.core.data_store.ObjectMetadata
import com.ibm.aspen.core.objects.{ObjectPointer, ObjectState}

trait OpportunisticRebuildManager {

  /** Informs the OpportunisticRebuildManager that the stores with pool indicies in repairNeeded are in
    * need of repairs
    */
  def markRepairNeeded(os: ObjectState, repairNeeded: Set[Byte]): Unit

  def getPreTransactionOpportunisticRebuild(pointer: ObjectPointer): Map[Byte, (ObjectMetadata, DataBuffer)]
}

object OpportunisticRebuildManager {

  object None extends OpportunisticRebuildManager {
    def markRepairNeeded(os: ObjectState, repairNeeded: Set[Byte]): Unit = ()

    def getPreTransactionOpportunisticRebuild(pointer: ObjectPointer): Map[Byte, (ObjectMetadata, DataBuffer)] = Map()
  }

}
