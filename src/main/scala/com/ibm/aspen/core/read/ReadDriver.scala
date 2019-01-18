package com.ibm.aspen.core.read

import java.util.UUID

import com.ibm.aspen.base.impl.TransactionStatusCache
import com.ibm.aspen.base.{ObjectCache, OpportunisticRebuildManager}
import com.ibm.aspen.core.network.ClientSideReadMessenger
import com.ibm.aspen.core.objects.{ObjectPointer, ObjectState}

import scala.concurrent.Future

trait ReadDriver {
  def readResult: Future[Either[ReadError, ObjectState]]
  
  /** Called to begin the read process. Read messages must not be sent until this method is called */
  def begin(): Unit
  
  /** Called to abandon the read. This calls should cancel all activity scheduled for the future */
  def shutdown(): Unit
  
  /** Returns True when all stores have been heard from */
  def receiveReadResponse(response:ReadResponse): Boolean

  val opportunisticRebuildManager: OpportunisticRebuildManager
}

object ReadDriver {

  /**
    * objectCache: ObjectCache,
    * opRebuildManager: OpportunisticRebuildManager,
    * transactionCache: TransactionStatusCache,
    * clientMessenger: ClientSideReadMessenger,
    * objectPointer: ObjectPointer,
    * readType: ReadType,
    * retrieveLockedTransaction: Boolean,
    * readUUID:UUID
    */
  type Factory = (ObjectCache, OpportunisticRebuildManager, TransactionStatusCache, ClientSideReadMessenger, ObjectPointer, ReadType,
    Boolean, UUID, Boolean) => ReadDriver

}