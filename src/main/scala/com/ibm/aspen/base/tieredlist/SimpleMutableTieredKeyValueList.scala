package com.ibm.aspen.base.tieredlist

import com.ibm.aspen.base.ObjectReader
import com.ibm.aspen.core.objects.KeyValueObjectPointer
import com.ibm.aspen.core.objects.keyvalue.KeyOrdering
import scala.concurrent.Future
import scala.concurrent.ExecutionContext
import com.ibm.aspen.core.objects.KeyValueObjectState
import com.ibm.aspen.core.objects.keyvalue.Key
import com.ibm.aspen.base.ObjectAllocater
import com.ibm.aspen.base.AspenSystem
import scala.concurrent.Promise
import scala.util.Failure
import scala.util.Success
import com.ibm.aspen.core.objects.keyvalue.KeyValueOperation
import com.ibm.aspen.core.objects.keyvalue.SetMin
import com.ibm.aspen.core.objects.keyvalue.SetMax
import com.ibm.aspen.core.objects.keyvalue.SetLeft
import com.ibm.aspen.core.objects.keyvalue.SetRight
import com.ibm.aspen.core.objects.keyvalue.Value
import com.ibm.aspen.base.Transaction
import com.ibm.aspen.core.objects.keyvalue.Insert
import com.ibm.aspen.core.objects.keyvalue.ByteArrayKeyOrdering

object SimpleMutableTieredKeyValueList {
  
  def load(
      system: AspenSystem,
      containerObject: KeyValueObjectPointer,
      treeKey: Key)(implicit ec: ExecutionContext): Future[SimpleMutableTieredKeyValueList] = {
    
    system.readObject(containerObject) map { kvos =>
      val root = TieredKeyValueList.Root(kvos.contents(treeKey).value)
      new SimpleMutableTieredKeyValueList(system, Left(containerObject), treeKey, root.keyOrdering, Some(root))
    }
  }
  
}

class SimpleMutableTieredKeyValueList(
    val system: AspenSystem,
    val treeContainer: Either[KeyValueObjectPointer, TieredKeyValueList.Root],
    val treeIdentifier: Key,
    val keyOrdering: KeyOrdering,
    initialRoot: Option[TieredKeyValueList.Root] = None) extends MutableTieredKeyValueList {
  
  def this(
      system: AspenSystem,
      containerObject: KeyValueObjectPointer,
      treeKey: Key,
      root: TieredKeyValueList.Root) = this(system, Left(containerObject), treeKey, root.keyOrdering, Some(root))
  
  val objectReader: ObjectReader = system
  
  private[this] var root: Option[TieredKeyValueList.Root] = initialRoot
  private[this] var allocaters: Map[Int, Future[ObjectAllocater]] = Map()
  
  override def destroy(prepareForDeletion: Map[Key,Value] => Future[Unit])(implicit ec: ExecutionContext): Future[Unit] = {
    val p = Promise[Unit]
    
    def rdelete(tiers: List[(Int, KeyValueListPointer)]): Unit = {
      if (tiers.isEmpty)
        p.success(())
      else {
        val (tier, pointer) = tiers.head
        
        def prepTier0(kvos: KeyValueObjectState): Future[Unit] = prepareForDeletion(kvos.contents)
        def prepRest(kvos: KeyValueObjectState): Future[Unit] = Future.unit
        
        val prepFunc = if (tier == 0) prepTier0 _ else prepRest _
        
        KeyValueList.destroy(system, pointer, prepFunc) foreach { _ => rdelete(tiers.tail) }
      }
    }
    
    getTierRootPointers onComplete {
      case Failure(cause) => p.failure(cause)
      case Success(tiers) => rdelete(tiers)
    }
    
    p.future
  }
  
  def getTierRootPointers()(implicit ec: ExecutionContext): Future[List[(Int, KeyValueListPointer)]] = {
    val p = Promise[List[(Int, KeyValueListPointer)]]()
    
    def rfind(ptr: KeyValueListPointer, tier: Int, tiers: List[(Int, KeyValueListPointer)]): Unit = {
      
      val thisTier = (tier, ptr) :: tiers
      
      if (tier == 0)
        p.success(thisTier)
      else {
        TieredKeyValueList.findPointerToNextTierDown(system, ptr, ByteArrayKeyOrdering, Set(), Key.AbsoluteMinimum) onComplete {
          case Failure(_) => p.success(thisTier) // This tier must already have been deleted
          
          case Success(e) => e match {
            case Left(_) => p.success(thisTier) // Must be in-process of deleting this tier
            
            case Right(nextTierPointer) => rfind(nextTierPointer, tier - 1, thisTier)
          }
        }
      }
    }
    
    rootPointer() onComplete { 
      case Failure(_) => p.success(Nil)
      
      case Success(root) => rfind(KeyValueListPointer(Key.AbsoluteMinimum, root.rootNode), root.topTier, Nil)
    }
    
    p.future
  }
  
  override protected[tieredlist] def prepreUpdateRootTransaction(
      newTier: Int,
      newRootPointer: KeyValueObjectPointer)(implicit tx: Transaction, ec: ExecutionContext): Future[Unit] = refreshRoot map { t =>
    val (kvos, root) = t
    
    var ops: List[KeyValueOperation] = Nil
    
    kvos.minimum.foreach( arr => ops = new SetMin(arr) :: ops )
    kvos.maximum.foreach( arr => ops = new SetMax(arr) :: ops )
    kvos.left.foreach( arr => ops = new SetLeft(arr) :: ops )
    kvos.right.foreach( arr => ops = new SetRight(arr) :: ops )
    
    val newRoot = TieredKeyValueList.Root(newTier, root.tierObjectAllocaters, root.tierNodeSizes, keyOrdering, newRootPointer)
    
    val updatedContent = kvos.contents + (treeIdentifier -> Value(treeIdentifier, newRoot.toArray(), tx.timestamp()))
    
    updatedContent.valuesIterator.foreach{ v => 
      ops = new Insert(v.key.bytes, v.value, v.timestamp) :: ops 
    }
    
    tx.overwrite(kvos.pointer, kvos.revision, Nil, ops)
  }
  
  protected def getObjectAllocaterForTier(tier: Int)(implicit ec: ExecutionContext): Future[ObjectAllocater] = synchronized {
    allocaters.get(tier) match {
      case Some(falloc) => falloc
      
      case None =>
        val p = Promise[ObjectAllocater]()
        allocaters += (tier -> p.future)
        
        rootPointer() onComplete {
          case Failure(cause) => p.failure(cause)
          case Success(root) => system.getObjectAllocater(root.getTierNodeAllocaterUUID(tier)) onComplete {
            case Failure(cause) => p.failure(cause)
            case Success(allocater) => p.success(allocater)
          }
        }
        
        p.future
    }
  }
  
  protected[tieredlist] def refreshRoot()(implicit ec: ExecutionContext): Future[(KeyValueObjectState, TieredKeyValueList.Root)] = {
    
    val fkvos = treeContainer match {
      case Left(pointer) => objectReader.readObject(pointer)
      case Right(root) => new SimpleTieredKeyValueList(objectReader, root, keyOrdering).fetchContainingNode(treeIdentifier, 0)
    }
    
    fkvos.map { kvos => kvos.contents.get(treeIdentifier) match {
        case None => throw new Exception(s"Broken Tree Container. No tree with ID: ${treeIdentifier}")
        case Some(v) => 
          val rt = TieredKeyValueList.Root(v.value)
          synchronized {
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