package com.ibm.aspen.core.transaction

import com.ibm.aspen.core.objects.ObjectPointer
import com.ibm.aspen.core.objects.ObjectRevision
import com.ibm.aspen.core.objects.ObjectRefcount

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