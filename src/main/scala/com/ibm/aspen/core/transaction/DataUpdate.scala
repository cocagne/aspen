package com.ibm.aspen.core.transaction

import com.ibm.aspen.core.objects.ObjectPointer
import com.ibm.aspen.core.objects.ObjectRevision

case class DataUpdate(
    objectPointer: ObjectPointer, 
    requiredRevision: ObjectRevision, 
    operation: DataUpdateOperation.Value) 