package com.ibm.aspen.base.tieredlist

import com.ibm.aspen.base.ObjectReader

class ReadOnlyTieredKeyValueList(
    val reader: ObjectReader,
    val rootManager: TieredKeyValueListRootManager) extends TieredKeyValueList {
 
  protected[tieredlist] def getObjectReaderForTier(tier: Int): ObjectReader = reader
  
}