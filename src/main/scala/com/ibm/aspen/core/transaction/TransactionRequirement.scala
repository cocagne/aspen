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
    operation: DataUpdateOperation.Value,
    updateMetadata: Boolean) extends TransactionRequirement
    
case class RefcountUpdate(
    objectPointer: ObjectPointer, 
    requiredRefcount: ObjectRefcount, 
    newRefcount: ObjectRefcount) extends TransactionRequirement
    
case class VersionBump(
    objectPointer: ObjectPointer, 
    requiredRevision: ObjectRevision) extends TransactionRequirement
    
sealed abstract class KeyValueTransactionRequirement extends TransactionRequirement {
    override val objectPointer: KeyValueObjectPointer
}
    
case class KeyValueTimestampRequirement(
    objectPointer: KeyValueObjectPointer,
    requirements: List[KeyValueTimestampRequirement.KVReq],
    timestamp: HLCTimestamp) extends KeyValueTransactionRequirement
    
object KeyValueTimestampRequirement {
  
  object TimestampRequirement extends Enumeration {
    val Equals       = Value("Equals")
    val LessThan     = Value("LessThen")
    val Exists       = Value("Exists")
    val DoesNotExist = Value("DoesNotExist")
  }
  
  case class KVReq(key: Key, tsRequirement: TimestampRequirement.Value)
}