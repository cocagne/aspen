package com.ibm.aspen.base.tieredlist

import com.ibm.aspen.base.ObjectReader
import com.ibm.aspen.core.objects.KeyValueObjectPointer
import com.ibm.aspen.core.objects.keyvalue.KeyOrdering
import scala.concurrent.Future
import scala.concurrent.ExecutionContext
import com.ibm.aspen.core.objects.KeyValueObjectState
import com.ibm.aspen.core.objects.keyvalue.Key

class SimpleMutableTieredKeyValueList(
    val objectReader: ObjectReader,
    val containingObjectTieredListDepth: Int,
    val containingObject: KeyValueObjectPointer,
    val treeIdentifier: Key,
    val keyOrdering: KeyOrdering) extends TieredKeyValueList {
  
  private[this] var rootContainer: Option[KeyValueObjectState] = None
  private[this] var root: Option[TieredKeyValueList.Root] = None
  
  private def refreshRoot()(implicit ec: ExecutionContext): Future[(KeyValueObjectState, TieredKeyValueList.Root)] = {
    
    val fkvos = if (containingObjectTieredListDepth < 0)
      objectReader.readObject(containingObject)
    else {
      val l = new SimpleTieredKeyValueList(objectReader, containingObjectTieredListDepth, containingObject, keyOrdering)
      l.fetchContainingNode(treeIdentifier, 0)
    }
    
    fkvos.map { kvos => kvos.contents.get(treeIdentifier) match {
        case None => throw new Exception("Broken Tree Container")
        case Some(v) => 
          val rt = TieredKeyValueList.Root.fromArray(v.value)
          synchronized {
            rootContainer = Some(kvos)
            root = Some(rt)
          }
          (kvos, rt)
      }
    }
  }
  
  override protected def rootPointer()(implicit ec: ExecutionContext): Future[TieredKeyValueList.Root] = synchronized {
    root match {
      case Some(r) => Future.successful(r)
      case None => refreshRoot().map(t => t._2)
    }
  }
  
  override protected def getObjectReaderForTier(tier: Int): ObjectReader = objectReader
}