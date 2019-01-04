package com.ibm.aspen.base.impl

import java.util.UUID

import com.github.blemale.scaffeine.{Cache, Scaffeine}
import com.ibm.aspen.core.data_store.DataStoreID

import scala.concurrent.duration.{Duration, SECONDS}

object TransactionStatusCache {
  private sealed abstract class TxStatus

  private class Aborted extends TxStatus

  /** ourCommitErrors contains a list of object UUIDs for which we didn't meet the transaction requirements and couldn't commit */
  private class Committed(var ourCommitErrors: Map[DataStoreID, List[UUID]] = Map()) extends TxStatus

  private class Finalized extends TxStatus
}

class TransactionStatusCache(cacheDuration: Duration = Duration(30, SECONDS)) {

  import TransactionStatusCache._

  private val transactionCache: Cache[UUID,TxStatus] = Scaffeine()
    .expireAfterWrite(cacheDuration)
    .build[UUID, TxStatus]()

  def transactionAborted(txuuid: UUID): Unit = transactionCache.put(txuuid, new Aborted)

  def transactionCommitted(txuuid: UUID): Unit = synchronized {
    transactionCache.getIfPresent(txuuid) match {
      case None => transactionCache.put(txuuid, new Committed())
      case Some(_) =>
    }
  }

  def onLocalCommit(txuuid: UUID, storeId: DataStoreID, commitErrors: List[UUID]): Unit = synchronized {
    transactionCache.getIfPresent(txuuid) match {
      case None => transactionCache.put(txuuid, new Committed(Map(storeId -> commitErrors)))
      case Some(status) => status match {
        case c: Committed => c.ourCommitErrors += (storeId -> commitErrors)
        case _ =>
      }
    }
  }

  def transactionFinalized(txuuid: UUID): Unit = transactionCache.put(txuuid, new Finalized)

  def getTransactionResolution(txuuid: UUID): Option[(Boolean, Option[Map[DataStoreID, List[UUID]]])] = transactionCache.getIfPresent(txuuid).map {
    case _: Aborted => (false, None)
    case c: Committed => synchronized {
      (true, Some(c.ourCommitErrors))
    }
    case _ => (true, None)
  }

  def getTransactionFinalizedResult(txuuid: UUID): Option[Boolean] = transactionCache.getIfPresent(txuuid).flatMap {
    case _: Aborted => Some(false)
    case _: Committed => None
    case _: Finalized => Some(true)
  }
}
