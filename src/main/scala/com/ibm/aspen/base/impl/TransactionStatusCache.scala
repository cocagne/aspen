package com.ibm.aspen.base.impl

import java.util.UUID

import com.github.blemale.scaffeine.{Cache, Scaffeine}

import scala.concurrent.duration.{Duration, SECONDS}

object TransactionStatusCache {
  sealed abstract class TxStatus

  case class Aborted() extends TxStatus

  case class Committed() extends TxStatus

  case class Finalized() extends TxStatus
}

class TransactionStatusCache(cacheDuration: Duration = Duration(30, SECONDS)) {

  import TransactionStatusCache._

  private val transactionCache: Cache[UUID,TxStatus] = Scaffeine()
    .expireAfterWrite(cacheDuration)
    .build[UUID, TxStatus]()

  def transactionAborted(txuuid: UUID): Unit = transactionCache.put(txuuid, Aborted())

  def transactionCommitted(txuuid: UUID): Unit = transactionCache.put(txuuid, Committed())

  def transactionFinalized(txuuid: UUID): Unit = transactionCache.put(txuuid, Finalized())

  def getTransactionResolved(txuuid: UUID): Option[Boolean] = transactionCache.getIfPresent(txuuid).map {
    case _: Aborted => false
    case _ => true
  }

  def getTransactionComplete(txuuid: UUID): Option[Boolean] = transactionCache.getIfPresent(txuuid).flatMap {
    case _: Aborted => Some(false)
    case _: Committed => None
    case _: Finalized => Some(true)
  }
}
