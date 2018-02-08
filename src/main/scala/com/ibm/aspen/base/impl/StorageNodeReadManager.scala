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
import com.ibm.aspen.core.data_store.InvalidByteRange
import com.ibm.aspen.core.read.SingleKey
import com.ibm.aspen.core.objects.keyvalue.KeyValueObjectStoreState
import com.ibm.aspen.core.read.LargestKeyLessThan
import com.ibm.aspen.core.objects.keyvalue.Key
import com.ibm.aspen.core.objects.keyvalue.KeyComparison
import com.ibm.aspen.core.read.KeyRange
import com.ibm.aspen.core.objects.keyvalue.Value
import com.ibm.aspen.core.read.LargestKeyLessThanOrEqualTo

class StorageNodeReadManager(messenger: StoreSideReadMessenger)(implicit ec: ExecutionContext) extends StoreSideReadMessageReceiver {
  
  private[this] var stores = Map[DataStoreID, DataStore]()
  
  private def getStore(sid: DataStoreID) = synchronized { stores.get(sid) }
  
  def hostedStores: List[DataStore] = synchronized { stores.values.toList }
  
  def addStore(store: DataStore): Unit = synchronized { stores += (store.storeId -> store) }
      
  def receive(message: Read): Unit = getStore(message.toStore).foreach { store => 

    def updateSet(db: DataBuffer): Set[UUID] = message.objectPointer match {
      case _: KeyValueObjectPointer => KeyValueObjectCodec.getUpdateSet(db)
      case _ => Set()
    }
    
    def sendErrorResponse(err: ObjectReadError): Unit = {
      val e = err match {
        case e: InvalidLocalPointer => ReadError.InvalidLocalPointer
        case e: ObjectMismatch => ReadError.ObjectMismatch
        case e: CorruptedObject => ReadError.CorruptedObject
        case e: InvalidByteRange => ReadError.InvalidByteRange
      }
      messenger.send(message.fromClient, ReadResponse(message.toStore, message.readUUID, Left(e)))
    }
    
    def respond(md: ObjectMetadata, updates: Set[UUID], sizeOnStore: Int, odata: Option[DataBuffer], locks: List[Lock]): Unit = {
      val cs = ReadResponse.CurrentState(md.revision, updates, md.refcount, md.timestamp, sizeOnStore, odata,
                                         if (message.returnLockedTransaction) locks else Nil)
              
        messenger.send(message.fromClient, ReadResponse(message.toStore, message.readUUID, Right(cs)))
    }

    message.readType match {
      case rt: MetadataOnly => store.getObjectMetadata(message.objectPointer) foreach { result => result match {
        case Left(err) => sendErrorResponse(err)
        case Right((metadata, locks)) => respond(metadata, Set(), 0, None, locks)
      }}
        
      case rt: FullObject => store.getObject(message.objectPointer) foreach { result => result match {
        case Left(err) => sendErrorResponse(err)
        case Right((metadata, data, locks)) => respond(metadata, updateSet(data), data.size, Some(data), locks)
      }} 
      
      case rt: ByteRange => store.getObject(message.objectPointer) foreach { result => result match {
        case Left(err) => sendErrorResponse(err)
        case Right((metadata, data, locks)) => 
          if (rt.offset + rt.length <= data.size)
            respond(metadata, Set(), data.size, Some(data.slice(rt.offset, rt.length)), locks)
          else
            sendErrorResponse(new InvalidByteRange)
      }}
      
      case rt: SingleKey => store.getObject(message.objectPointer) foreach { result => result match {
        case Left(err) => sendErrorResponse(err)
        case Right((metadata, data, locks)) =>
          try {
            val kvos = KeyValueObjectStoreState(0, data)
            
            val includeMinMax = !kvos.keyInRange(rt.key, rt.comparison)
            
            val values = kvos.idaEncodedContents.get(rt.key) match {
              case Some(v) => List(v)
              case None => Nil
            }
            
            val partialData = KeyValueObjectCodec.encodePartialRead(kvos, includeMinMax=includeMinMax, kvlist=values)
            respond(metadata, updateSet(data), data.size, Some(partialData), locks)  
          } catch {
            case _: Throwable => sendErrorResponse(new CorruptedObject)
          }
      }}
      
      case rt: LargestKeyLessThan => store.getObject(message.objectPointer) foreach { result => result match {
        case Left(err) => sendErrorResponse(err)
        case Right((metadata, data, locks)) =>
          try {
            val kvos = KeyValueObjectStoreState(0, data)
            
            val (includeMinMax, kvlist: List[Value]) = if (kvos.keyInRange(rt.key, rt.comparison)) {
              val init: Option[Value] = None
              val okey = kvos.idaEncodedContents.foldLeft(init){ (o,t) => o match {
                case None => if (rt.comparison(t._1, rt.key) < 0) Some(t._2) else None
                case Some(lv) => if (rt.comparison(t._1, rt.key) < 0 && rt.comparison(t._1, lv.key) > 0) Some(t._2) else Some(lv)
              }}
              (okey.isEmpty, okey.toList) 
            } else {
              (true, Nil)
            }
            
            val partialData = KeyValueObjectCodec.encodePartialRead(kvos, includeMinMax=includeMinMax, kvlist=kvlist)
            respond(metadata, updateSet(data), data.size, Some(partialData), locks)  
          } catch {
            case _: Throwable => sendErrorResponse(new CorruptedObject)
          }
      }}
      
      case rt: LargestKeyLessThanOrEqualTo => store.getObject(message.objectPointer) foreach { result => result match {
        case Left(err) => sendErrorResponse(err)
        case Right((metadata, data, locks)) =>
          try {
            val kvos = KeyValueObjectStoreState(0, data)
            
            val (includeMinMax, kvlist: List[Value]) = if (kvos.keyInRange(rt.key, rt.comparison)) {
              val init: Option[Value] = None
              val okey = kvos.idaEncodedContents.foldLeft(init){ (o,t) => o match {
                case None => if (rt.comparison(t._1, rt.key) <= 0) Some(t._2) else None
                case Some(lv) => if (rt.comparison(t._1, rt.key) <= 0 && rt.comparison(t._1, lv.key) > 0) Some(t._2) else Some(lv)
              }}
              (okey.isEmpty, okey.toList) 
            } else {
              (true, Nil)
            }
            
            val partialData = KeyValueObjectCodec.encodePartialRead(kvos, includeMinMax=includeMinMax, kvlist=kvlist)
            respond(metadata, updateSet(data), data.size, Some(partialData), locks)  
          } catch {
            case _: Throwable => sendErrorResponse(new CorruptedObject)
          }
      }}
      
      case rt: KeyRange => store.getObject(message.objectPointer) foreach { result => result match {
        case Left(err) => sendErrorResponse(err)
        case Right((metadata, data, locks)) =>
          try {
            val kvos = KeyValueObjectStoreState(0, data)

            val kvlist = kvos.idaEncodedContents.foldLeft(List[Value]()){ (l, t) =>
              if ( rt.comparison(t._1, rt.minimum) >= 0 && rt.comparison(t._1, rt.maximum) <= 0 ) 
                t._2 :: l
              else
                l
            }
            
            val partialData = KeyValueObjectCodec.encodePartialRead(kvos, includeMinMax=true, kvlist=kvlist)
            respond(metadata, updateSet(data), data.size, Some(partialData), locks)  
          } catch {
            case _: Throwable => sendErrorResponse(new CorruptedObject)
          }
      }}
    }
  }
}