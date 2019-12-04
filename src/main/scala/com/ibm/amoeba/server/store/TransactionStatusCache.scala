package com.ibm.amoeba.server.store

import com.github.blemale.scaffeine.{Cache, Scaffeine}
import com.ibm.amoeba.common.transaction.{TransactionId, TransactionStatus}

import scala.concurrent.duration.{Duration, SECONDS}

object TransactionStatusCache {
  case class Entry(status: TransactionStatus.Value, finalized: Boolean)
}

class TransactionStatusCache(cacheDuration: Duration = Duration(30, SECONDS)) {
  import TransactionStatusCache._

  private val cache: Cache[TransactionId,Entry] = Scaffeine()
    .expireAfterWrite(cacheDuration)
    .build[TransactionId, Entry]()

  def updateStatus(txid: TransactionId, status: TransactionStatus.Value, finalized: Boolean=false): Unit = {
    cache.put(txid, Entry(status, finalized))
  }

  def getStatus(txid: TransactionId): Option[Entry] = {
    cache.getIfPresent(txid)
  }
}
