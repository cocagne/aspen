package com.ibm.aspen.core.read

import scala.concurrent.ExecutionContext
import com.ibm.aspen.core.network.ClientSideReadMessenger
import com.ibm.aspen.core.objects.ObjectPointer
import java.util.UUID

class TriggeredReread {
  
  var active = Set[TriggerDriver]()
  
  def addActive(td: TriggerDriver) = synchronized { active += td }
  
  def delActive(td: TriggerDriver) = synchronized { active -= td }
  
  def retry() = synchronized { active.foreach( _.retry() ) }
  
  class TriggerDriver(
    clientMessenger: ClientSideReadMessenger,
    objectPointer: ObjectPointer,
    retrieveObjectData: Boolean,
    retrieveLockedTransaction: Boolean, 
    readUUID:UUID)(implicit ec: ExecutionContext) extends BaseReadDriver(clientMessenger, objectPointer, 
        retrieveObjectData, retrieveLockedTransaction, readUUID)(ec) {
    
    addActive(this)
    
    readResult.onComplete { case _ => delActive(this) }
    
    def retry() = sendReadRequests()
  }
  
  def triggeredReadDriver(ec: ExecutionContext)(
      clientMessenger: ClientSideReadMessenger,
      objectPointer: ObjectPointer,
      retrieveObjectData: Boolean,
      retrieveLockedTransaction: Boolean,
      readUUID:UUID): ReadDriver = {
    val rd = new TriggerDriver(clientMessenger, objectPointer, retrieveObjectData, retrieveLockedTransaction, readUUID)(ec)
    rd.start()
    rd
  }
}