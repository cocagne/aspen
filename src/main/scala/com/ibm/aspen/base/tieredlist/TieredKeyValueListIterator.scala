package com.ibm.aspen.base.tieredlist

import scala.concurrent.ExecutionContext
import com.ibm.aspen.base.ObjectReader
import scala.concurrent.Future
import com.ibm.aspen.core.objects.keyvalue.Value
import com.ibm.aspen.core.objects.keyvalue.Key
import com.ibm.aspen.core.objects.KeyValueObjectState
import scala.concurrent.Promise
import com.ibm.aspen.core.objects.KeyValueObjectPointer
import scala.util.Success
import scala.util.Failure

/** This class iterates over all of the Key/Value pairs found in a single left-to-right pass
 *  through the tier-0 list. Keys added/removed during the iteration process may be missed but
 *  all static, unmodified pairs will be found. 
 *  
 *  Split operations performed on a node being processed are not an issue as all of the keys
 *  that can be placed in the newly-split node will either be new additions, which we don't try
 *  to find anyway, or pairs that were already in the node that we're processing. Splits, on the
 *  other hand require a bit more work as they result in existing keys shifting into nodes to
 *  the left of the join target. To handle this, we rely on the atomicity guarantee of the join
 *  operation deleting the node being removed. This will cause the fetch of the deleted node to
 *  return an error. When this occurs, we attempt to re-load and re-process the current node. 
 *  Any keys shifted into the current node will then be processed as normal. Should the current
 *  node _also_ have been deleted, the re-read of the current node will also fail. To handle
 *  this case, we start all over from the top of the tree by fetching whichever node currently
 *  owns the range containing the highest processed key.
 */
class TieredKeyValueListIterator(
    val reader: ObjectReader,
    val tree: TieredKeyValueList)(implicit ec: ExecutionContext) {
  
  private[this] var valueListPromise = Promise[Unit]()
  private[this] var highestProcessedKey = Key.AbsoluteMinimum
  private[this] var valueList: List[Value] = Nil
  private[this] var currentNode: Option[KeyValueObjectState] = None
  private[this] var onextPromise: Option[Promise[Option[Value]]] = None
  private[this] var done = false
  
  private def loadCurrentNode(kvos: KeyValueObjectState): Unit = synchronized {
    
    currentNode = Some(kvos)
    
    if (kvos.right.isEmpty)
      done = true
      
    // filter out all keys less than the highestProcessedKey, convert to list, and sort
    valueList = kvos.contents.values.
                filter(v => tree.keyOrdering.compare(v.key, highestProcessedKey) > 0).
                toList.
                sortWith( (a,b) => tree.keyOrdering.compare(a.key,b.key) < 0 )
    
    if (valueList.isEmpty) {
      kvos.right match {
        case None => valueListPromise.success(()) // All KV pairs consumed. We're done
        case Some(right) => fetchRight(right.content)
      }
    } else {
      val p = valueListPromise
      valueListPromise = Promise[Unit]()
      p.success(())
    }
  }
  
  private def fetchRight(ptrArray: Array[Byte]): Unit = {
    reader.readObject(KeyValueObjectPointer(ptrArray)) onComplete {
      case Success(kvos) => loadCurrentNode(kvos)
      
      case Failure(err) => // Node probably deleted due to a join. Re-read the current object to pick up any migrated keys
        val currentPtr = synchronized { currentNode.get.pointer }
        
        reader.readObject(currentPtr) onComplete {
          case Success(kvos) => loadCurrentNode(kvos)
          
          case Failure(err) => // Current node must have been deleted as well. Restart from the top of the tree
            loadNextNodeFromTree()
        }
    }
  }
  
  private def loadNextNodeFromTree(): Unit = synchronized {
    tree.fetchContainingNode(highestProcessedKey, targetTier=0).foreach(kvos => loadCurrentNode(kvos))
  }
  
  private def popValue(): Option[Value] = {
    val n = valueList.head
    valueList = valueList.tail
    highestProcessedKey = n.key
    Some(n)
  }
  
  def next(): Future[Option[Value]] = synchronized {
    if (!valueList.isEmpty) {
      Future.successful(popValue())
    } else {
      if (done)
        Future.successful(None)
      else {
        onextPromise match {
          case Some(p) => p.future
          
          case None =>
            val npromise = Promise[Option[Value]]()
            onextPromise = Some(npromise)
            
            currentNode match {
              case None => loadNextNodeFromTree() // Initial request
              case Some(kvos) => fetchRight(kvos.right.get.content) // kvos.right must be valid or done would be true
            }
            
            valueListPromise.future.foreach { _ => synchronized {
              onextPromise = None
              
              if (valueList.isEmpty)
                npromise.success(None)
              else
                npromise.success(popValue())
            }}
            
            npromise.future
        }
      }
    }
  }
}