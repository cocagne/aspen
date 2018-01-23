package com.ibm.aspen.core.data_store

import java.util.UUID


class MutableKeyValueObject(
    objectId: StoreObjectID, 
    initialOperation: UUID, 
    loader: MutableObjectLoader) extends MutableObject(objectId, initialOperation, loader) {
  
}