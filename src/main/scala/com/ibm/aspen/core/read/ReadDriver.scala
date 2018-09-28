package com.ibm.aspen.core.read

import java.util.UUID

import com.ibm.aspen.core.network.ClientSideReadMessenger
import com.ibm.aspen.core.objects.{ObjectPointer, ObjectState}

import scala.concurrent.Future
import scala.concurrent.duration.Duration

trait ReadDriver {
  def readResult: Future[Either[ReadError, ObjectState]]
  
  /** Called to begin the read process. Read messages must not be sent until this method is called */
  def begin(): Unit
  
  /** Called to abandon the read. This calls should cancel all activity scheduled for the future */
  def shutdown(): Unit
  
  /** Returns True when all stores have been heard from */
  def receiveReadResponse(response:ReadResponse): Boolean
  
  val opportunisticRebuildDelay: Duration
}

object ReadDriver {
  /**
   * Signature: 
   * 
   * Factory( clientMessenger: ClientSideReadMessenger
   *          objectPointer: ObjectPointer,
   *          readType: ReadType,
   *          retrieveLockedTransaction: Boolean,
   * ,        readUUID:UUID,
   *          disableOpportunisticRebuild: Boolean)
   */
  type Factory = (UUID => Option[Boolean], ClientSideReadMessenger, ObjectPointer, ReadType, Boolean, UUID, Boolean) => ReadDriver
}