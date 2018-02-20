package com.ibm.aspen.base.tieredlist

import java.util.UUID
import com.ibm.aspen.base.Transaction
import com.ibm.aspen.core.objects.keyvalue.Key
import com.ibm.aspen.core.objects.KeyValueObjectPointer
import scala.concurrent.Future
import com.ibm.aspen.base.impl.BaseCodec
import com.ibm.aspen.base.RetryStrategy
import com.ibm.aspen.base.AspenSystem
import com.ibm.aspen.base.FinalizationActionHandler
import com.ibm.aspen.base.FinalizationAction
import scala.concurrent.ExecutionContext

object TieredKeyValueListSplitFA {
  val FinalizationActionUUID = UUID.fromString("09d1c4a2-22a7-48b0-85f4-726afea02f2a")
  
  /** 
   * @param transaction Transaction to add the FinalizationAction to
   * @param treeIdentifier Unique identifer for the tree root key-value pair
   * @param containingObject KeyValue object containing the treeIdentifier OR a pointer to a TieredList containing the key-value pair
   * @param containingObjectTieredListDepth if < 0, the pointer is to a single object that must contain the treeIdentifier. If >0, the
   *        containingObject points to a TieredList and this parameter is the tier number of the root pointer
   * @param targetTier the tier number into which the new node pointer should be inserted
   * @param newNode the new node to be inserted into the tier
   */
  def addFinalizationAction(
      transaction: Transaction, 
      treeIdentifier: Key, containingObject: KeyValueObjectPointer, containingObjectTieredListDepth: Int, 
      targetTier: Int, newNode: KeyValueListPointer): Unit = {
    
    val serializedContent = BaseCodec.encodeTieredKeyValueListSplitFA(treeIdentifier, containingObject, containingObjectTieredListDepth,
        targetTier, newNode)
        
    transaction.addFinalizationAction(FinalizationActionUUID, serializedContent)
  }
  
  case class Content(
      treeIdentifier: Key, 
      containingObject: KeyValueObjectPointer, 
      containingObjectTieredListDepth: Int, 
      targetTier: Int, 
      newNode: KeyValueListPointer) 
}

abstract class TieredKeyValueListSplitFA(
    val retryStrategy: RetryStrategy,
    val system: AspenSystem) extends FinalizationActionHandler {
  
  import TieredKeyValueListSplitFA._
  
  val finalizationActionUUID: UUID = FinalizationActionUUID
  /*
  class InsertIntoUpperTier(val c: Content) extends FinalizationAction {
    def execute()(implicit ec: ExecutionContext): Future[Unit] = retryStrategy.retryUntilSuccessful {
      Future.unit
     
      val containerKvos = if (c.containingObjectTieredListDepth == -1) {
        system.readObject(c.containingObject)
      } else {
        TieredKeyValueList.Root(c.containingObjectTieredListDepth, c.containingObject)
      }
     
    }
  }
  * 
  */
  
}