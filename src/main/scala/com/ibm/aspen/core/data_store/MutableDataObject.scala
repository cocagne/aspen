package com.ibm.aspen.core.data_store

import java.util.UUID

class MutableDataObject( 
    objectId: StoreObjectID, 
    initialOperation: UUID, 
    loader: MutableObjectLoader) extends MutableObject(objectId, initialOperation, loader) {}
