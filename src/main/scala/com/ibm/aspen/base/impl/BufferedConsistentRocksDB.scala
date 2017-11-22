package com.ibm.aspen.base.impl

import org.rocksdb.RocksDB
import org.rocksdb.Options
import org.rocksdb.WriteBatch
import scala.concurrent.ExecutionContext
import scala.concurrent.Future
import scala.concurrent.Promise
import org.rocksdb.WriteOptions

object BufferedConsistentRocksDB {
  case class DBClosed() extends Throwable
}

/** RocksDB Key-Value store were all puts & deletes return Futures to data-at-rest on disk.
 *  
 *  All blocking put/get/delete operations on the RocksDB database are delegated to Future {} blocks handled by the implicit
 *  ExecutionContext. This should be tuned to support the desired number of background threads.
 *  
 *  While a synchronous commit is outstanding, all put/delete operations are buffered in a WriteBatch. When the outstanding
 *  commit completes, the commit of the next write batch is immediately started if it contains any operations. When a put/delete
 *  is done and no outstanding commit exists, a commit for that single operation is immediately started. 
 * 
 */
class BufferedConsistentRocksDB(
    val dbPath:String)(implicit ec: ExecutionContext) {
  
  import BufferedConsistentRocksDB._
  
  private[this] val db: RocksDB = {
    val options = new Options().setCreateIfMissing(true)
    try {
      RocksDB.open(options, dbPath)
    } finally {
      options.close()
    }
  }
  
  private[this] var nextBatch = new WriteBatch()
  private[this] var nextPromise = Promise[Unit]()
  private[this] var commitInProgress = false
  private[this] var closing: Option[Promise[Unit]] = None
  private[this] var outsandingOpCount = 0
  
  private[this] def doNextCommit() = {
    val nbatch = nextBatch
    val npromise = nextPromise
    nextBatch = new WriteBatch()
    nextPromise = Promise[Unit]()
    commitInProgress = true  
    commit(nbatch, npromise)
  }
  
  private[this] def beginOperation() = synchronized {
    if (closing.isDefined) throw DBClosed()
    outsandingOpCount += 1
  }
  private[this] def endOperation() = synchronized {
    outsandingOpCount -= 1
    if (outsandingOpCount == 0)
      closing.foreach(p => p.success(()))
  }
  
  
  private[this] def commit(batch:WriteBatch, promise: Promise[Unit]): Unit = Future {
     
    val writeOpts = new WriteOptions()
    writeOpts.setSync(true)
    
    beginOperation()
    try {
      db.write(writeOpts, batch)
    } finally {
      endOperation()
      writeOpts.close()
      batch.close()
    }
    
    promise.success(())
    
    // Chain through to the next commit if pending writes exist
    synchronized { 
      if (nextBatch.count() > 0)
        doNextCommit()
      else 
        commitInProgress = false
    }
  }
  
  
  def put(key: Array[Byte], value: Array[Byte]): Future[Unit] = synchronized {
    nextBatch.put(key, value)
    val fcommit = nextPromise.future
    
    if (!commitInProgress) 
      doNextCommit()
    
    fcommit
  }
  
  def delete(key: Array[Byte]): Future[Unit] = synchronized {
    nextBatch.remove(key)
    val fcommit = nextPromise.future
    
    if (!commitInProgress)
      doNextCommit()
    
    fcommit
  }
  
  def get(key: Array[Byte]): Future[Option[Array[Byte]]] = Future { 
    beginOperation()
    val value = db.get(key)
    endOperation()
    
    if (value == null)
      None
    else
      Some(value)
  }
  
  def foreach(fn: (Array[Byte], Array[Byte]) => Unit): Future[Unit] = Future {
    beginOperation()
    val iterator = db.newIterator()
    try {
      iterator.seekToFirst()
      while (iterator.isValid()) {
        fn(iterator.key(), iterator.value())
        iterator.next()
      }
    } finally {
      endOperation()
      iterator.close()
    }
  }
  
  def close(): Future[Unit] = synchronized {
    
    val p = Promise[Unit]()
    closing = Some(p)
    
    if (outsandingOpCount == 0)
      p.success(())
    
    p.future.andThen{ case _ => 
      db.close()
      nextBatch.close()
    }
  }
}