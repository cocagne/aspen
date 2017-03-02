package com.ibm.aspen.core.data_store

import scala.concurrent._

import com.ibm.aspen.core.transaction.TransactionDescription
import java.util.UUID
import com.ibm.aspen.core.transaction.LocalUpdateContent
import com.ibm.aspen.core.objects.ObjectRevision
import com.ibm.aspen.core.objects.ObjectRefcount

/* A do-nothing store that simply returns empty successes. Use this as a base class for 
 * mock stores used in tests. The "stored" objects have ObjectRevision(1,10
 */
class NullDataStore(val storeId: DataStoreID) extends DataStore {
  
  import NullDataStore._
  
  def getCurrentObjectState(txd: TransactionDescription): Future[ Map[UUID, Either[ObjectError.Value, CurrentObjectState]] ] = {
    var m = Map[UUID, Either[ObjectError.Value, CurrentObjectState]]()
    txd.dataUpdates.foreach(du => m += (du.objectPointer.uuid -> Right(CurrentObjectState(du.objectPointer.uuid, revision, refcount))))
    txd.dataUpdates.foreach(ru => m += (ru.objectPointer.uuid -> Right(CurrentObjectState(ru.objectPointer.uuid, revision, refcount))))
    Future.successful(m)
  }
  
  def lockOrCollide(txd: TransactionDescription): Option[Map[UUID, TransactionDescription]] = None
  
  def commitTransactionUpdates(txd: TransactionDescription, localUpdates: LocalUpdateContent): Future[Unit] = Future.successful(())
  
  def discardTransaction(txd: TransactionDescription): Unit = ()
}

object NullDataStore {
  val revision = ObjectRevision(1,10)
  val refcount = ObjectRefcount(1,1)
}