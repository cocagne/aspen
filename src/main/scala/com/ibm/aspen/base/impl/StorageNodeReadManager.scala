package com.ibm.aspen.base.impl

import scala.concurrent.ExecutionContext
import com.ibm.aspen.core.network.StoreSideReadMessageReceiver
import com.ibm.aspen.core.data_store.DataStoreID
import com.ibm.aspen.core.data_store.DataStore
import com.ibm.aspen.core.network.StoreSideReadMessenger
import com.ibm.aspen.core.transaction.TransactionRecoveryState
import com.ibm.aspen.core.allocation.AllocationRecoveryState
import scala.concurrent.Future
import com.ibm.aspen.core.read.Read
import com.ibm.aspen.core.data_store.InvalidLocalPointer
import com.ibm.aspen.core.data_store.ObjectMismatch
import com.ibm.aspen.core.data_store.CorruptedObject
import com.ibm.aspen.core.read.ReadError
import com.ibm.aspen.core.read.ReadResponse
import com.ibm.aspen.core.objects.KeyValueObjectPointer
import java.util.UUID
import com.ibm.aspen.core.objects.keyvalue.KeyValueObjectCodec
import com.ibm.aspen.core.data_store.ObjectReadError
import com.ibm.aspen.core.DataBuffer
import com.ibm.aspen.core.read.MetadataOnly
import com.ibm.aspen.core.data_store.Lock
import com.ibm.aspen.core.data_store.ObjectMetadata
import com.ibm.aspen.core.read.FullObject
import com.ibm.aspen.core.read.ByteRange
import com.ibm.aspen.core.read.SingleKey
import com.ibm.aspen.core.objects.keyvalue.KeyValueObjectStoreState
import com.ibm.aspen.core.read.LargestKeyLessThan
import com.ibm.aspen.core.objects.keyvalue.Key
import com.ibm.aspen.core.objects.keyvalue.KeyOrdering
import com.ibm.aspen.core.read.KeyRange
import com.ibm.aspen.core.objects.keyvalue.Value
import com.ibm.aspen.core.read.LargestKeyLessThanOrEqualTo
import com.ibm.aspen.core.read.OpportunisticRebuild
import com.ibm.aspen.core.HLCTimestamp

class StorageNodeReadManager(messenger: StoreSideReadMessenger)(implicit ec: ExecutionContext) extends StoreSideReadMessageReceiver {
  
  private[this] var stores = Map[DataStoreID, DataStore]()
  
  private def getStore(sid: DataStoreID) = synchronized { stores.get(sid) }
  
  def hostedStores: List[DataStore] = synchronized { stores.values.toList }
  
  def addStore(store: DataStore): Unit = synchronized { stores += (store.storeId -> store) }
  
  def receive(message: OpportunisticRebuild): Unit = getStore(message.toStore).foreach { store => 
    store.opportunisticRebuild(message)
  }
      
  def receive(message: Read): Unit = getStore(message.toStore).foreach { store => 

    def sendErrorResponse(err: ObjectReadError): Unit = {
      messenger.send(message.fromClient, ReadResponse(message.toStore, message.readUUID, HLCTimestamp.now, Left(ObjectReadError(err))))
    }
    
    def partialKvoss(values: List[Value]): DataBuffer = {
      new KeyValueObjectStoreState(None, None, None, None, values.map(v => (v.key -> v)).toMap).encode()
    }
    
    def respond(md: ObjectMetadata, sizeOnStore: Int, odata: Option[DataBuffer], locks: List[Lock]): Unit = {
      val cs = ReadResponse.CurrentState(md.revision, md.refcount, md.timestamp, sizeOnStore, odata,
                                         if (message.returnLockedTransaction) locks else Nil)
              
        messenger.send(message.fromClient, ReadResponse(message.toStore, message.readUUID, HLCTimestamp.now, Right(cs)))
    }

    message.readType match {
      case rt: MetadataOnly => store.getObjectMetadata(message.objectPointer) foreach { result => result match {
        case Left(err) => sendErrorResponse(err)
        case Right((metadata, locks)) => respond(metadata, 0, None, locks)
      }}
        
      case rt: FullObject => store.getObject(message.objectPointer) foreach { result => result match {
        case Left(err) => sendErrorResponse(err)
        case Right((metadata, data, locks)) => respond(metadata, data.size, Some(data), locks)
      }} 
      
      case rt: ByteRange => store.getObject(message.objectPointer) foreach { result => result match {
        case Left(err) => sendErrorResponse(err)
        case Right((metadata, data, locks)) => 
          if (rt.offset + rt.length <= data.size)
            respond(metadata, data.size, Some(data.slice(rt.offset, rt.length)), locks)
          else if (rt.offset < data.size)
            respond(metadata, data.size, Some(data.slice(rt.offset)), locks)
          else
            respond(metadata, data.size, Some(DataBuffer.Empty), locks)
      }}
      
      case rt: SingleKey => store.getObject(message.objectPointer) foreach { result => result match {
        case Left(err) => sendErrorResponse(err)
        case Right((metadata, data, locks)) =>
          try {
            val kvos = KeyValueObjectStoreState.decode(data)
            
            val includeMinMax = !kvos.keyInRange(rt.key, rt.ordering)
            
            val values = kvos.idaEncodedContents.get(rt.key) match {
              case Some(v) => List(v)
              case None => Nil
            }
            
            val partialData = partialKvoss(values)
            respond(metadata, data.size, Some(partialData), locks)  
          } catch {
            case _: Throwable => sendErrorResponse(new CorruptedObject)
          }
      }}
      
      case rt: LargestKeyLessThan => store.getObject(message.objectPointer) foreach { result => result match {
        case Left(err) => sendErrorResponse(err)
        case Right((metadata, data, locks)) =>
          try {
            val kvos = KeyValueObjectStoreState.decode(data)
            
            val (includeMinMax, kvlist: List[Value]) = if (kvos.keyInRange(rt.key, rt.ordering)) {
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
            respond(metadata, data.size, Some(partialData), locks)  
          } catch {
            case _: Throwable => sendErrorResponse(new CorruptedObject)
          }
      }}
      
      case rt: LargestKeyLessThanOrEqualTo => store.getObject(message.objectPointer) foreach { result => result match {
        case Left(err) => sendErrorResponse(err)
        case Right((metadata, data, locks)) =>
          try {
            val kvos = KeyValueObjectStoreState.decode(data)
            
            val (includeMinMax, kvlist: List[Value]) = if (kvos.keyInRange(rt.key, rt.ordering)) {
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
            respond(metadata, data.size, Some(partialData), locks)  
          } catch {
            case _: Throwable => sendErrorResponse(new CorruptedObject)
          }
      }}
      
      case rt: KeyRange => store.getObject(message.objectPointer) foreach { result => result match {
        case Left(err) => sendErrorResponse(err)
        case Right((metadata, data, locks)) =>
          try {
            val kvos = KeyValueObjectStoreState.decode(data)

            val kvlist = kvos.idaEncodedContents.foldLeft(List[Value]()){ (l, t) =>
              if ( rt.ordering.compare(t._1, rt.minimum) >= 0 && rt.ordering.compare(t._1, rt.maximum) <= 0 ) 
                t._2 :: l
              else
                l
            }
            
            val partialData = partialKvoss(kvlist)
            respond(metadata, data.size, Some(partialData), locks)  
          } catch {
            case _: Throwable => sendErrorResponse(new CorruptedObject)
          }
      }}
    }
  }
}