package com.ibm.aspen.core.transaction

import com.ibm.aspen.core.objects.ObjectPointer
import com.ibm.aspen.core.objects.ObjectRevision
import com.ibm.aspen.core.objects.ObjectRefcount
import com.ibm.aspen.core.objects.KeyValueObjectPointer
import com.ibm.aspen.core.objects.keyvalue.Key
import com.ibm.aspen.core.HLCTimestamp

sealed abstract class TransactionRequirement {
  val objectPointer: ObjectPointer
}

case class DataUpdate(
    objectPointer: ObjectPointer, 
    requiredRevision: ObjectRevision, 
    operation: DataUpdateOperation.Value) extends TransactionRequirement
    
case class RefcountUpdate(
    objectPointer: ObjectPointer, 
    requiredRefcount: ObjectRefcount, 
    newRefcount: ObjectRefcount) extends TransactionRequirement
    
case class VersionBump(
    objectPointer: ObjectPointer, 
    requiredRevision: ObjectRevision) extends TransactionRequirement
    
case class RevisionLock(
    objectPointer: ObjectPointer, 
    requiredRevision: ObjectRevision) extends TransactionRequirement
    
sealed abstract class KeyValueTransactionRequirement extends TransactionRequirement {
    override val objectPointer: KeyValueObjectPointer
}
    
case class KeyValueUpdate(
    objectPointer: KeyValueObjectPointer,
    updateType: KeyValueUpdate.UpdateType.Value,
    requiredRevision: Option[ObjectRevision],
    requirements: List[KeyValueUpdate.KVRequirement],
    timestamp: HLCTimestamp) extends KeyValueTransactionRequirement
    
object KeyValueUpdate {
  
  object UpdateType extends Enumeration {
    //val Overwrite = Value("Overwrite")
    val Update    = Value("Update")
  }
  
  object TimestampRequirement extends Enumeration {
    val Equals       = Value("Equals")
    val LessThan     = Value("LessThan")
    val Exists       = Value("Exists")
    val DoesNotExist = Value("DoesNotExist")
  }
  
  case class KVRequirement(key: Key, timestamp: HLCTimestamp, tsRequirement: TimestampRequirement.Value)
}