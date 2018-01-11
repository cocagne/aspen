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
import com.ibm.aspen.core.network.NetworkCodec
import scala.annotation.tailrec

object KVTree {
  
  object KeyComparison extends Enumeration {
    val Raw = Value
    val BigInt = Value  // Keys are encoded via BigInteger.toByteArray
    val Lexical = Value // Keys are UTF-8 encoded strings
  }
  
  def rawCompare(a: Array[Byte], b: Array[Byte]): Int = {
    for (i <- 0 until a.length) {
      if (i > b.length) return 1 // a is longer than b and all preceeding bytes are equal
      if (a(i) < b(i)) return -1 // a is less than b
      if (a(i) > b(i)) return 1  // a is greater than b
    }
    if (b.length > a.length) return -1 // b is longer than a and all preceeding bytes are equal
    0 // a and b are the same length and have matching content
  }
  
  def bigIntCompare(a: Array[Byte], b: Array[Byte]): Int = {
    if (a.length == 0 && b.length == 0)
      0
    else if (a.length == 0 && b.length != 0)
      -1
    else if (a.length != 0 && b.length == 0)
      1
    else {
      val bigA = new java.math.BigInteger(a)
      val bigB = new java.math.BigInteger(b)
      bigA.compareTo(bigB)
    }
  }
  
  def lexicalCompare(a: Array[Byte], b: Array[Byte]): Int = {
    val sa = new String(a, "UTF-8")
    val sb = new String(b, "UTF-8")
    sa.compareTo(sb)
  }
  
  def getKeyComparisonFunction(keyType: KeyComparison.Value): (Array[Byte], Array[Byte]) => Int = keyType match {
    case KeyComparison.Raw => rawCompare
    case KeyComparison.BigInt => bigIntCompare
    case KeyComparison.Lexical => lexicalCompare
  }
  
  /** Creates the content for writing in to a new TreeDefinition object */
  def defineNewTree(allocationPolicyUUID: UUID, keyComparison: KVTree.KeyComparison.Value): Array[Byte] = {
    val td = KVTreeDefinition(allocationPolicyUUID, keyComparison, Nil)
    KVTreeCodec.encodeTreeDefinition(td)
  }
  
  /** Bootstrapping method for defining a tree with an initial tier0 node */
  def defineNewTreeWithInitialTier0Node(
      allocationPolicyUUID: UUID, 
      keyComparison: KVTree.KeyComparison.Value, 
      tier0HeadNode: ObjectPointer): Array[Byte] = {
    val td = KVTreeDefinition(allocationPolicyUUID, keyComparison, tier0HeadNode :: Nil)
    KVTreeCodec.encodeTreeDefinition(td)
  }
}

class KVTree(
    val treeDefinitionPointer: ObjectPointer, // Pointer to object holding the serialized content of this instance
    private[this] var treeDefinitionRevision: ObjectRevision,
    val nodeAllocater: KVTreeNodeAllocater,
    val nodeCache: KVTreeNodeCache,
    val keyComparisonStrategy: KVTree.KeyComparison.Value,
    rootPointers: List[ObjectPointer],
    val system: AspenSystem) {
  
  import KVTree._
  
  val compareKeysFunction = getKeyComparisonFunction(keyComparisonStrategy)
  
  class Tier(val tier: Int, val rootObjectPointer: ObjectPointer) extends KVList {
   
    val objectAllocater = nodeAllocater.getListNodeAllocaterForTier(tier)
    
    def fetchNodeObject(objectPointer: ObjectPointer): Future[ObjectStateAndData] = readObject(objectPointer)
    
    override def fetchCachedNode(objectPointer: ObjectPointer): Option[KVListNode] = nodeCache.getCachedNode(tier, objectPointer)
    override def updateCachedNode(node: KVListNode): Unit = nodeCache.updateCachedNode(tier, node)
    override def dropCachedNode(node: KVListNode): Unit = nodeCache.dropCachedNode(node)
    
    def fetchRoot()(implicit ec: ExecutionContext): Future[KVListNode] = fetchNode(KVListNodePointer(rootObjectPointer, new Array[Byte](0)))
    
    def compareKeys(a: Array[Byte], b: Array[Byte]): Int = compareKeysFunction(a, b)
  }
  
  private[this] var tiers:Array[Tier] = rootPointers.zipWithIndex.map(t => new Tier(t._2, t._1)).toArray
  private[this] var creatingTier: Option[Future[ObjectPointer]] = None
  
  def get(key: Array[Byte])(implicit ec: ExecutionContext): Future[Option[Array[Byte]]] = fetchContainingNode(key) map {
    tpl => tpl._2.content.get(key)
  }
  
  def uncachedGet(key: Array[Byte])(implicit ec: ExecutionContext): Future[Option[Array[Byte]]] = {
    val promise = Promise[Option[Array[Byte]]]()
    
    def findNode(): Unit = {
      fetchContainingNode(key) onComplete {
        case Failure(cause) => promise.failure(cause)
        case Success((tier, staleNode)) =>
                
          // The process of refreshing a node ensures that it's previously cached value is expired. If we're terribly
          // out of date, several recursive expirations may be required
          staleNode.refresh() onComplete {
            case Failure(cause) => promise.failure(cause)
            case Success(freshNode) =>
              if (!freshNode.keyWithinRange(key))
                findNode()
              else
                promise.success(freshNode.content.get(key))
          }
      }
    }
    
    findNode()

    promise.future
  }
  
  /* Future completes when the transaction is ready to commit */
  def put(key: Array[Byte], value: Array[Byte])(implicit ec: ExecutionContext, t: Transaction): Future[Unit] = {
    val pcommitReady = Promise[Unit]()
    fetchContainingNode(key) onComplete {
      case Failure(cause) => pcommitReady.failure(cause)
      case Success(tpl) => 
        tpl._2.update((key,value)::Nil, Nil, onListNodeSplit(0)) onComplete {
          case Success(_) => pcommitReady.success(())
          case Failure(cause) => pcommitReady.failure(cause)
        }
    }
    pcommitReady.future
  }
  
  def visitRange(
      startKey: Array[Byte], 
      stopKey: Option[Array[Byte]], 
      visitor: (Array[Byte], Array[Byte]) => Unit)(implicit ec: ExecutionContext): Future[Unit] = {
    val p = Promise[Unit]()
    fetchContainingNode(startKey, targetTier=0) onComplete {
      case Failure(cause) => p.failure(cause)
      case Success(root) => 
        def visit(node: KVListNode): Unit = {
          node.content.foreach( t => visitor(t._1, t._2) )
          
          val nextNode = node.rightNode match {
            case None => None
            case Some(rp) => stopKey match {
                case None => Some(rp)
                case Some(stop) => if (compareKeysFunction(stop, rp.minimum ) < 0) Some(rp) else None
              }
          }
              
          nextNode match {
            case None => p.success(())
            case Some(nextPointer) => tiers(0).fetchNode(nextPointer) onComplete {
              case Failure(cause) => p.failure(cause)
              case Success(nextNode) => visit(nextNode)
            }
          }
        }
        
        visit(root._2)
    }
    p.future
  }
  
  /* Intended primarily for inserting newly allocated object pointers into the tree.
   * 
   * genValue - function that will be called with the ObjectPointer and ObjectRevision of the tree node that owns the key space
   *            for the supplied key
   *            
   * Future completes with the generated value when the transaction is ready to commit. 
   */
  def putGeneratedValueIntoTreeNode(
      key: Array[Byte], 
      genValue: (ObjectPointer, ObjectRevision) => Future[Array[Byte]])(implicit ec: ExecutionContext, t: Transaction): Future[Array[Byte]] = {
    val pcommitReady = Promise[Array[Byte]]()
    fetchContainingNode(key) onComplete {
      case Failure(cause) => pcommitReady.failure(cause)
      case Success(tpl) =>
        val (_, node) = tpl
        genValue(node.nodePointer.objectPointer, node.nodeRevision) onComplete {
          case Failure(cause) => pcommitReady.failure(cause)
          case Success(value) =>
            tpl._2.update((key,value)::Nil, Nil, onListNodeSplit(0)) onComplete {
              case Success(_) => pcommitReady.success(value)
              case Failure(cause) => pcommitReady.failure(cause)
            }
        }
    }
    pcommitReady.future
  }
  
  def delete(key: Array[Byte])(implicit ec: ExecutionContext, t: Transaction): Future[Unit] = fetchContainingNode(key) map {
    tpl => tpl._2.update(Nil, key::Nil, onListNodeSplit(0))
  }

  /** Reads and returns the underlying object.
   *
   * This is broken out into a dedicated method primarily to allow mix-in traits to override the default behavior  
   */
  def readObject(objectPointer: ObjectPointer): Future[ObjectStateAndData] = system.readObject(objectPointer, None)
  
  protected [kvtree] def onListNodeSplit(tier: Int)(transaction:Transaction, ec:ExecutionContext, originalNode:KVListNode, updatedNode:KVListNode, newNode:KVListNode): Unit = {
    KVTreeFinalizationActionHandler.insertIntoUpperTier(transaction, this, tier+1, newNode.nodePointer)
    // TODO: Add callback that inserts newNode into the upper tier or drops the cached upper tier node?
    //       Could just rely on cache expiry times
  }
   
  def refresh()(implicit ec: ExecutionContext): Future[Unit] = system.readObject(treeDefinitionPointer, None) map { osd =>
    val td = KVTreeCodec.decodeTreeDefinition(osd.data)
    synchronized {
      if (osd.revision > treeDefinitionRevision) {
        tiers = td.tiers.zipWithIndex.map(t => new Tier(t._2, t._1)).toArray
        treeDefinitionRevision = osd.revision 
      }
    }
  }
  
  protected [kvtree] def rootTier: Tier = synchronized { tiers(tiers.length-1) }
  
  def numTiers = synchronized { tiers.length }
  
  protected def getTier(tier: Int) = synchronized { tiers(tier) }
  
  protected [kvtree] def createNextTier(initialContent: List[KVListNodePointer])(implicit ec: ExecutionContext): Future[ObjectPointer] = synchronized {
    creatingTier match {
      case Some(f) => f
      
      case None =>
        val p = Promise[ObjectPointer]()
    
        implicit val tx = system.newTransaction()
        val requiredRevision = treeDefinitionRevision // Snapshot this value. Could change underneath us
        
        val tier = tiers.length // Tier to create. Tiers start counting at zero so the length of the array is the tier number to create
        
        val falloc = if (tier == 0) 
            nodeAllocater.allocateRootLeafNode(treeDefinitionPointer, requiredRevision) 
          else 
            nodeAllocater.allocateRootTierNode(treeDefinitionPointer, requiredRevision, tier, initialContent)
            
        falloc onComplete {
            case Success(ptr) => synchronized {
              // Node allocation was successful. Update the tree definition node to embed a reference to it and commit the transaction
              val newTiers = new Array[Tier](tier + 1)
              tiers.copyToArray(newTiers)
              newTiers(tier) = new Tier(tier, ptr)
              val newTD = KVTreeDefinition(nodeAllocater.allocationPolicyUUID, KeyComparison.Raw, newTiers.iterator.map(_.rootObjectPointer).toList)
              val data = KVTreeCodec.encodeTreeDefinition(newTD)
              
              tx.overwrite(treeDefinitionPointer, requiredRevision, data)
              
              tx.commit() onComplete {
                case Success(_) => synchronized {
                  tiers = newTiers
                  treeDefinitionRevision = requiredRevision.nextRevision
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
  
  /** Called when the tree structure is horribly broken and no paths from the root node can be found
   *  
   *  The solution is to start from the root of the target tier and scan right until the target node is found.
   */
  protected def navigationFallbackOfLastResort(targetTier: Int, key: Array[Byte])(implicit ec: ExecutionContext): Future[KVListNode] = {
    getTier(targetTier).fetchRoot() flatMap {root => root.fetchContainingNode(key)}
  }
  
  protected [kvtree] def fetchContainingNode(key: Array[Byte], targetTier:Int=0)(implicit ec: ExecutionContext): Future[(Tier, KVListNode)] = {
    val p = Promise[(Tier, KVListNode)]()
    
    def fetchLower(
        t: Tier, startingNode: KVListNode, blacklisted: Set[KVListNodePointer], 
        path: List[(Tier, KVListNode)], initialReverseOrder: Option[List[(Array[Byte], Array[Byte])]]): Unit = {
      
      // Scan to the right to find the correct owning node
      startingNode.fetchContainingNode(key, blacklisted) onComplete {
        case Failure(cause) => p.failure(cause) // propagate to caller
        
        case Success(node) => 
          if (t.tier == targetTier)
            p.success((t, node))
          else {
            
            // Returns a list of all pointers in this node that are less than or equal to the target key in reverse sorted order
            @tailrec
            def getReverseOrder(forward: Iterator[(Array[Byte], Array[Byte])], reverse: List[(Array[Byte], Array[Byte])]): List[(Array[Byte], Array[Byte])] = {
              if (!forward.hasNext)
                reverse
              else {
                val kv = forward.next()
                if (compareKeysFunction(key, kv._1) >= 0)
                  getReverseOrder(forward, kv :: reverse)
                else
                  reverse
              }
            }
            
            @tailrec
            def findFirstNonBlacklistedNode(remainingPointers: List[(Array[Byte], Array[Byte])]): (Option[KVListNodePointer], List[(Array[Byte], Array[Byte])]) = {
              if (remainingPointers.isEmpty)
                (None, Nil)
              else {
                val (minimum, encodedObjectPointer) = remainingPointers.head
                val np =  KVListNodePointer(NetworkCodec.byteArrayToObjectPointer(encodedObjectPointer), minimum)
                if (!blacklisted.contains(np))
                  (Some(np), remainingPointers.tail)
                else
                  findFirstNonBlacklistedNode(remainingPointers.tail)
              }
            }
            
            def lastResort() = {
              navigationFallbackOfLastResort(targetTier, key) onComplete {
                case Failure(cause) => p.failure(cause)
                case Success(targetNode) => p.success((getTier(targetTier), targetNode)) 
              }
            }
            
            def blacklistNodeAndResumeSearchFromParent() = {
              if (path.isEmpty) 
                lastResort()
              else {
                val (parentTier, parentNode) = path.head
                fetchLower(parentTier, parentNode, blacklisted + node.nodePointer, path.tail, None)
              }
            }
            
            val reverseOrder = initialReverseOrder match {
              case Some(ro) => ro
              case None => getReverseOrder(node.content.iterator, Nil)
            }
            
            val (lowerNodePointer, remainingPointers) = findFirstNonBlacklistedNode(reverseOrder)
            
            lowerNodePointer match {
              case None => blacklistNodeAndResumeSearchFromParent()
                
              case Some(np) => t.fetchNode(np) onComplete {
                case Success(lowerNode) => fetchLower(getTier(t.tier-1), lowerNode, blacklisted, (t, node) :: path, None)
                
                case Failure(cause) => 
                  // TODO Analyze cause and schedule repairs if needed
                  if (remainingPointers.isEmpty)  
                      blacklistNodeAndResumeSearchFromParent
                  else 
                    fetchLower(t, startingNode, blacklisted, path, Some(remainingPointers))
              }
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
            fetchLower(root, rn, Set(), Nil, None)
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

 