package com.ibm.aspen.base.kvtree

import com.ibm.aspen.core.objects.ObjectPointer
import com.ibm.aspen.core.objects.ObjectRevision
import java.util.UUID
import com.ibm.aspen.base.AspenSystem
import scala.concurrent.ExecutionContext
import scala.concurrent.Future
import scala.concurrent.Promise
import scala.util.Success
import scala.util.Failure
import java.nio.ByteBuffer
import com.ibm.aspen.base.Transaction
import com.ibm.aspen.base.kvlist.KVListNode
import com.ibm.aspen.base.kvlist.KVList
import com.ibm.aspen.base.ObjectStateAndData
import com.ibm.aspen.base.kvlist.KVListNodeAllocater
import com.ibm.aspen.base.kvlist.KVListNodePointer
import com.ibm.aspen.core.network.{Codec => NetworkCodec}
import scala.annotation.tailrec

abstract class KVTreeImpl(
    val treeDescriptionPointer: ObjectPointer, // Pointer to object holding the serialized content of this instance
    private[this] var treeDescriptionRevision: ObjectRevision,
    val nodeAllocater: KVTreeNodeAllocater,
    val nodeCache: KVTreeNodeCache,
    val compareKeysFunction: (Array[Byte], Array[Byte]) => Int,
    private[this] var rootPointers: List[ObjectPointer],
    val system: AspenSystem) {
  
  class Tier(val tier: Int, val rootObjectPointer: ObjectPointer) extends KVList {
   
    val objectAllocater = nodeAllocater.getListNodeAllocaterForTier(tier)
    
    def fetchNodeObject(objectPointer: ObjectPointer): Future[ObjectStateAndData] = readObject(objectPointer)
    
    override def fetchCachedNode(objectPointer: ObjectPointer): Option[KVListNode] = nodeCache.getCachedNode(tier, objectPointer)
    override def updateCachedNode(node: KVListNode): Unit = nodeCache.updateCachedNode(tier, node)
    override def dropCachedNode(node: KVListNode): Unit = nodeCache.dropCachedNode(node)
    
    def fetchRoot()(implicit ec: ExecutionContext): Future[KVListNode] = fetchNode(rootObjectPointer, new Array[Byte](0), None)
    
    def compareKeys(a: Array[Byte], b: Array[Byte]): Int = compareKeysFunction(a, b)
  }
  
  private[this] var tiers:Array[Tier] = rootPointers.zipWithIndex.map(t => new Tier(t._2, t._1)).toArray
  private[this] var creatingTier: Option[Future[ObjectPointer]] = None
  
  def get(key: Array[Byte])(implicit ec: ExecutionContext): Future[Option[Array[Byte]]] = fetchContainingNode(key) map {
    tpl => tpl._2.content.get(key)
  }
  
  def put(key: Array[Byte], value: Array[Byte])(implicit ec: ExecutionContext, t: Transaction): Future[Unit] = fetchContainingNode(key) map {
    tpl => tpl._2.update((key,value)::Nil, Nil, onListNodeSplit(tpl._1.tier))
  }
  
  def delete(key: Array[Byte])(implicit ec: ExecutionContext, t: Transaction): Future[Unit] = fetchContainingNode(key) map {
    tpl => tpl._2.update(Nil, key::Nil, onListNodeSplit(tpl._1.tier))
  }

  /** Reads and returns the underlying object.
   *
   * This is broken out into a dedicated method primarily to allow mix-in traits to override the default behavior  
   */
  def readObject(objectPointer: ObjectPointer): Future[ObjectStateAndData] = system.readObject(objectPointer, None)
  
  protected def onListNodeSplit(tier: Int)(transaction:Transaction, ec:ExecutionContext, originalNode:KVListNode, updatedNode:KVListNode, newNode:KVListNode): Unit = {
    KVTreeFinalizationActions.insertIntoUpperTier(transaction, tier+1, newNode.nodePointer)
    // TODO: Add callback to successful commit to do the operation and inform the designated leader
  }
   
  def refresh()(implicit ec: ExecutionContext): Future[Unit] = system.readObject(treeDescriptionPointer, None) map { osd =>
    val (_, tierPointers) = KVTreeCodec.decodeTreeDescription(osd.data)
    synchronized {
      if (osd.revision > treeDescriptionRevision) {
        tiers = tierPointers.zipWithIndex.map(t => new Tier(t._2, t._1)).toArray
        treeDescriptionRevision = osd.revision 
      }
    }
  }
  
  protected def rootTier: Tier = synchronized { tiers(tiers.length-1) }
  
  protected def getTier(tier: Int) = synchronized { tiers(tier) }
  
  protected def createNextTier(initialContent: List[KVListNodePointer])(implicit ec: ExecutionContext): Future[ObjectPointer] = synchronized {
    creatingTier match {
      case Some(f) => f
      
      case None =>
        val p = Promise[ObjectPointer]()
    
        implicit val tx = system.newTransaction()
        val requiredRevision = treeDescriptionRevision // Snapshot this value. Could change underneath us
        
        val tier = tiers.length // Tier to create. Tiers start counting at zero so the length of the array is the tier number to create
        
        val falloc = if (tier == 0) 
            nodeAllocater.allocateRootLeafNode(treeDescriptionPointer, requiredRevision) 
          else 
            nodeAllocater.allocateRootTierNode(treeDescriptionPointer, requiredRevision, tier, initialContent)
            
        falloc onComplete {
            case Success(ptr) => synchronized {
              // Node allocation was successful. Update the tree description node to embed a reference to it and commit the transaction
              val newTiers = new Array[Tier](tier + 1)
              tiers.copyToArray(newTiers)
              newTiers(tier) = new Tier(tier, ptr)
              val data = KVTreeCodec.encodeTreeDescription(nodeAllocater.allocationPolicyUUID, newTiers.iterator.map(_.rootObjectPointer).toList)
              
              tx.overwrite(treeDescriptionPointer, requiredRevision, data)
              
              tx.commit() onComplete {
                case Success(_) => synchronized {
                  tiers = newTiers
                  treeDescriptionRevision = requiredRevision.overwrite(data.length)
                  p.success(ptr)
                }
                case Failure(reason) => p.failure(reason)
              }
            }
            
            case Failure(reason) => synchronized { 
              tx.invalidateTransaction(reason)
              p.failure(reason)
            }
        }
        
        creatingTier = Some(p.future)
        
        p.future onComplete {
          case _ => synchronized { creatingTier = None }
        }
        
        p.future
      }
    }
  
  protected def fetchContainingNode(key: Array[Byte], targetTier:Int=0)(implicit ec: ExecutionContext): Future[(Tier, KVListNode)] = {
    val p = Promise[(Tier, KVListNode)]()
    
    def fetchLower(t: Tier, startingNode: KVListNode, blacklisted: Set[KVListNodePointer], path: List[(Tier, KVListNode)]): Unit = {
      startingNode.fetchContainingNode(key, blacklisted) onComplete {
        case Failure(cause) => p.failure(cause) // propagate to caller
        case Success(node) => 
          if (t.tier == targetTier)
            p.success((t, node))
          else {
            // Returns a list of all pointers in this node that are less than or equal to the target key 
            @tailrec
            def getLowerPointers(kvi: Iterator[(Array[Byte], Array[Byte])], reverseOrder:List[(Array[Byte], Array[Byte])]): List[(Array[Byte], Array[Byte])] = {
              if (!kvi.hasNext)
                reverseOrder
              else {
                val kv = kvi.next
                if (compareKeysFunction(key, kv._1) > 0)
                  reverseOrder
                else
                  getLowerPointers(kvi, kv :: reverseOrder)
              }
            }
            
            val it = getLowerPointers(node.content.iterator, Nil).iterator
            
            while (it.hasNext) {
              val (minimum, encodedObjectPointer) = it.next()
              val np =  KVListNodePointer(NetworkCodec.byteArrayToObjectPointer(encodedObjectPointer), minimum)
              if (!blacklisted.contains(np)) {
                t.fetchNode(np.objectPointer, np.minimum, None) onComplete {
                  case Failure(cause) => p.failure(cause)
                  case Success(lowerNode) => 
                    fetchLower(getTier(t.tier-1), lowerNode, blacklisted, (t, node) :: path)
                    return 
                }
              }
            }
            //
            // If we get here then no useable pointers were found in this node. 
            // Blacklist this node and resume search from our parent tier
            //
            if (path.isEmpty) {
              // The tree structure is horribly broken (this really shouldn't be possible). 
              // Last resort is to do a full scan of the target tier
              val tgtTier = getTier(targetTier) 
              tgtTier.fetchRoot() onComplete {
                case Failure(cause) => p.failure(cause)
                case Success(rn) => rn.fetchContainingNode(key) onComplete {
                  case Failure(cause) => p.failure(cause)
                  case Success(targetNode) =>p.success((tgtTier, targetNode))
                }
              }
            } else {
              val (parentTier, parentNode) = path.head
              fetchLower(parentTier, parentNode, blacklisted + node.nodePointer, path.tail)
            }
        }
      }
    }
    
    def doFetch(root: Tier) {
      root.fetchRoot() onComplete {
        case Failure(cause) => p.failure(cause)
        case Success(rn) =>
          if (root.tier == targetTier) {
            rn.fetchContainingNode(key) onComplete {
              case Failure(cause) => p.failure(cause)
              case Success(targetNode) => p.success((root, targetNode))
            }
          } else {
            fetchLower(root, rn, Set(), Nil)
          }
      }
    }
    
    // If no tiers exist, create the first one. Otherwise do the fetch
    synchronized {
      if (tiers.length == 0) {
        createNextTier(Nil) onComplete {
          case Failure(cause) => p.failure(cause)
          case Success(_) => doFetch(tiers(0))
        }
      } else {
        doFetch(tiers(tiers.length-1))
      }
    }
    
    p.future
  }
    
} 

 