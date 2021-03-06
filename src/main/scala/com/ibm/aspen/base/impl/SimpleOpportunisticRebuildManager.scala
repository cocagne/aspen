package com.ibm.aspen.base.impl

import java.util.UUID

import com.github.blemale.scaffeine.Scaffeine
import com.ibm.aspen.base.{AspenSystem, OpportunisticRebuildManager}
import com.ibm.aspen.core.data_store.{DataStoreID, ObjectMetadata}
import com.ibm.aspen.core.objects.{ObjectPointer, ObjectState}
import com.ibm.aspen.core.transaction.PreTransactionOpportunisticRebuild

import scala.concurrent.duration._
import scala.concurrent.duration.Duration

class SimpleOpportunisticRebuildManager(system: AspenSystem) extends OpportunisticRebuildManager {

  private[this] val repairCache = Scaffeine().expireAfterWrite(Duration(10, SECONDS))
    .maximumSize(5000)
    .build[UUID, Set[Byte]]()

  val slowReadReplyDuration: Duration = Duration(5, SECONDS)

  def markRepairNeeded(os: ObjectState, repairNeeded: Set[Byte]): Unit = repairCache.put(os.pointer.uuid, repairNeeded)

  def getPreTransactionOpportunisticRebuild(pointer: ObjectPointer): Map[Byte, PreTransactionOpportunisticRebuild] = {
    repairCache.getIfPresent(pointer.uuid) match {
      case None => Map()
      case Some(set) => system.objectCache.get(pointer) match {
        case None => Map()
        case Some(os) =>
          set.foldLeft(Map[Byte, PreTransactionOpportunisticRebuild]()){ (m, i) =>
            os.getRebuildDataForStore(DataStoreID(pointer.poolUUID, i)) match {
              case None => m
              case Some(db) =>
                val p = new PreTransactionOpportunisticRebuild(pointer.uuid, ObjectMetadata(os.revision, os.refcount, os.timestamp), db)
                m + (i -> p)
            }
          }
      }
    }
  }
}
