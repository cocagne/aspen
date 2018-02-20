package com.ibm.aspen.base.tieredlist

import com.ibm.aspen.core.objects.keyvalue.KeyOrdering
import scala.concurrent.Future
import com.ibm.aspen.core.objects.KeyValueObjectPointer
import com.ibm.aspen.base.ObjectReader
import scala.concurrent.ExecutionContext

class SimpleTieredKeyValueList(
    val objectReader: ObjectReader,
    val topTier: Int,
    val root: KeyValueObjectPointer,
    val keyOrdering: KeyOrdering) extends TieredKeyValueList {
  
  override protected def rootPointer()(implicit ec: ExecutionContext): Future[TieredKeyValueList.Root] = Future.successful(TieredKeyValueList.Root(topTier, root))
  
  override protected def getObjectReaderForTier(tier: Int): ObjectReader = objectReader
}