package com.ibm.aspen.core.read

import com.ibm.aspen.core.objects.ObjectPointer
import com.ibm.aspen.core.objects.ObjectRevision
import com.ibm.aspen.core.objects.ObjectRefcount
import com.ibm.aspen.core.transaction.TransactionDescription
import com.ibm.aspen.core.data_store.DataStoreID
import java.nio.ByteBuffer
import com.ibm.aspen.core.DataBuffer

case class ObjectState(
    objectPointer: ObjectPointer,
    revision: ObjectRevision,
    refcount: ObjectRefcount,
    data: Option[DataBuffer],
    locks: Option[List[(DataStoreID,TransactionDescription)]])
