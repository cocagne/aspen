package com.ibm.aspen.core.read

import com.ibm.aspen.core.HLCTimestamp
import com.ibm.aspen.core.data_store.{DataStoreID, ObjectReadError}
import com.ibm.aspen.core.objects.{ObjectPointer, ObjectState}

trait ObjectReader {
  val pointer: ObjectPointer

  def rereadCandidates: Map[DataStoreID, HLCTimestamp]

  def result: Option[Either[ObjectReadError.Value, ObjectState]]

  def receiveReadResponse(response:ReadResponse): Option[Either[ObjectReadError.Value, ObjectState]]

  def numResponses: Int

  def receivedResponsesFromAllStores: Boolean = numResponses == pointer.ida.width

  def debugLogStatus(log: String => Unit): Unit
}
