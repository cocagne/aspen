package com.ibm.aspen.base.impl

import java.util.UUID

import com.ibm.aspen.core.{DataBuffer, HLCTimestamp}
import com.ibm.aspen.core.data_store._
import com.ibm.aspen.core.network.{StoreSideReadMessageReceiver, StoreSideReadMessenger}
import com.ibm.aspen.core.objects.keyvalue.Value
import com.ibm.aspen.core.read.{CorruptedObject => _, _}

import scala.concurrent.ExecutionContext

class StorageNodeReadManager(messenger: StoreSideReadMessenger)(implicit ec: ExecutionContext) extends StoreSideReadMessageReceiver {
  
  private[this] var stores = Map[DataStoreID, DataStore]()
  
  private def getStore(sid: DataStoreID) = synchronized { stores.get(sid) }
  
  def hostedStores: List[DataStore] = synchronized { stores.values.toList }
  
  def addStore(store: DataStore): Unit = synchronized { stores += (store.storeId -> store) }
  
  def receive(message: OpportunisticRebuild): Unit = getStore(message.toStore).foreach { store => 
    store.opportunisticRebuild(message)
  }

  def receive(message: TransactionCompletionQuery): Unit = getStore(message.toStore).foreach { store =>
    val isComplete = !store.transactionInProgress(message.transactionUUID)
    messenger.send(message.fromClient, TransactionCompletionResponse(message.toStore, message.queryUUID, isComplete))
  }
      
  def receive(message: Read): Unit = getStore(message.toStore).foreach { store => 

    def sendErrorResponse(err: ObjectReadError): Unit = {
      messenger.send(message.fromClient, ReadResponse(message.toStore, message.readUUID, HLCTimestamp.now, Left(ObjectReadError(err))))
    }
    
    def partialKvoss(values: List[Value]): DataBuffer = {
      new StoreKeyValueObjectContent(None, None, None, None, values.map(v => v.key -> v).toMap).encode()
    }
    
    def respond(md: ObjectMetadata, sizeOnStore: Int, odata: Option[DataBuffer], writeLocks: Set[UUID]): Unit = {
      val cs = ReadResponse.CurrentState(md.revision, md.refcount, md.timestamp, sizeOnStore, odata, writeLocks)
              
      messenger.send(message.fromClient, ReadResponse(message.toStore, message.readUUID, HLCTimestamp.now, Right(cs)))
    }

    message.readType match {
      case _: MetadataOnly => store.getObjectMetadata(message.objectPointer) foreach {
        case Left(err) => sendErrorResponse(err)
        case Right((metadata, _, writeLocks)) => respond(metadata, 0, None, writeLocks)
      }
        
      case _: FullObject => store.getObject(message.objectPointer) foreach {
        case Left(err) => sendErrorResponse(err)
        case Right((metadata, data, _, writeLocks)) => respond(metadata, data.size, Some(data), writeLocks)
      }
      
      case rt: ByteRange => store.getObject(message.objectPointer) foreach {
        case Left(err) => sendErrorResponse(err)
        case Right((metadata, data, _, writeLocks)) =>
          if (rt.offset + rt.length <= data.size)
            respond(metadata, data.size, Some(data.slice(rt.offset, rt.length)), writeLocks)
          else if (rt.offset < data.size)
            respond(metadata, data.size, Some(data.slice(rt.offset)), writeLocks)
          else
            respond(metadata, data.size, Some(DataBuffer.Empty), writeLocks)
      }
      
      case rt: SingleKey => store.getObject(message.objectPointer) foreach {
        case Left(err) => sendErrorResponse(err)
        case Right((metadata, data, _, writeLocks)) =>
          try {
            val kvos = StoreKeyValueObjectContent(data)

            val values = kvos.idaEncodedContents.get(rt.key) match {
              case Some(v) => List(v)
              case None => Nil
            }
            
            val partialData = partialKvoss(values)
            respond(metadata, data.size, Some(partialData), writeLocks)  
          } catch {
            case _: Throwable => sendErrorResponse(new CorruptedObject)
          }
      }
      
      case rt: LargestKeyLessThan => store.getObject(message.objectPointer) foreach {
        case Left(err) => sendErrorResponse(err)
        case Right((metadata, data, _, writeLocks)) =>
          try {
            val kvos = StoreKeyValueObjectContent(data)
            
            val (_, kvlist: List[Value]) = if (kvos.keyInRange(rt.key, rt.ordering)) {
              val init: Option[Value] = None
              val okey = kvos.idaEncodedContents.foldLeft(init){ (o,t) => o match {
                case None => if (rt.ordering.compare(t._1, rt.key) < 0) Some(t._2) else None
                case Some(lv) => if (rt.ordering.compare(t._1, rt.key) < 0 && rt.ordering.compare(t._1, lv.key) > 0) Some(t._2) else Some(lv)
              }}
              (okey.isEmpty, okey.toList) 
            } else {
              (true, Nil)
            }
            
            val partialData = partialKvoss(kvlist)
            respond(metadata, data.size, Some(partialData), writeLocks)  
          } catch {
            case _: Throwable => sendErrorResponse(new CorruptedObject)
          }
      }
      
      case rt: LargestKeyLessThanOrEqualTo => store.getObject(message.objectPointer) foreach {
        case Left(err) => sendErrorResponse(err)
        case Right((metadata, data, _, writeLocks)) =>
          try {
            val kvos = StoreKeyValueObjectContent(data)
            
            val (_, kvlist: List[Value]) = if (kvos.keyInRange(rt.key, rt.ordering)) {
              val init: Option[Value] = None
              val okey = kvos.idaEncodedContents.foldLeft(init){ (o,t) => o match {
                case None => if (rt.ordering.compare(t._1, rt.key) <= 0) Some(t._2) else None
                case Some(lv) => if (rt.ordering.compare(t._1, rt.key) <= 0 && rt.ordering.compare(t._1, lv.key) > 0) Some(t._2) else Some(lv)
              }}
              (okey.isEmpty, okey.toList) 
            } else {
              (true, Nil)
            }
            
            val partialData = partialKvoss(kvlist)
            respond(metadata, data.size, Some(partialData), writeLocks)  
          } catch {
            case _: Throwable => sendErrorResponse(new CorruptedObject)
          }
      }
      
      case rt: KeyRange => store.getObject(message.objectPointer) foreach {
        case Left(err) => sendErrorResponse(err)
        case Right((metadata, data, _, writeLocks)) =>
          try {
            val kvos = StoreKeyValueObjectContent(data)

            val kvlist = kvos.idaEncodedContents.foldLeft(List[Value]()){ (l, t) =>
              if ( rt.ordering.compare(t._1, rt.minimum) >= 0 && rt.ordering.compare(t._1, rt.maximum) <= 0 ) 
                t._2 :: l
              else
                l
            }
            
            val partialData = partialKvoss(kvlist)
            respond(metadata, data.size, Some(partialData), writeLocks)  
          } catch {
            case _: Throwable => sendErrorResponse(new CorruptedObject)
          }
      }
    }
  }
}