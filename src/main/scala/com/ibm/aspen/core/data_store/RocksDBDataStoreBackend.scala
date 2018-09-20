package com.ibm.aspen.core.data_store

import java.nio.ByteBuffer
import java.util.UUID

import com.ibm.aspen.base.impl.BufferedConsistentRocksDB
import com.ibm.aspen.core.{DataBuffer, HLCTimestamp}
import com.ibm.aspen.core.allocation.AllocationErrors
import com.ibm.aspen.core.objects.{ObjectRefcount, ObjectRevision}

import scala.concurrent.{ExecutionContext, Future}

object RocksDBDataStoreBackend {
  val NullArray = new Array[Byte](0)
  val MetadataIndex:Byte = 0
  val DataIndex:Byte = 1
  
  private [this] def tokey(objectUUID:UUID, index:Byte) = {
    val bb = ByteBuffer.allocate(17)
    bb.putLong(0, objectUUID.getMostSignificantBits)
    bb.putLong(8, objectUUID.getLeastSignificantBits)
    bb.put(16, index)
    bb.array()
  }
  
  private def metadataKey(objectId:StoreObjectID) = tokey(objectId.objectUUID, MetadataIndex)
  private def dataKey(objectId:StoreObjectID) = tokey(objectId.objectUUID, DataIndex)
  
  private def metadataToBytes(metadata: ObjectMetadata): Array[Byte] = {
    val bb = ByteBuffer.allocate(32)
    bb.putLong(0, metadata.revision.lastUpdateTxUUID.getMostSignificantBits)
    bb.putLong(8, metadata.revision.lastUpdateTxUUID.getLeastSignificantBits)
    bb.putInt(16, metadata.refcount.updateSerial)
    bb.putInt(20, metadata.refcount.count)
    bb.putLong(24,metadata.timestamp.asLong)
    bb.array()
  }
  private def bytesToMetadata(buf:Array[Byte]): ObjectMetadata = {
    val bb = ByteBuffer.wrap(buf)
    val rev = ObjectRevision(new UUID(bb.getLong(0), bb.getLong(8)))
    val ref = ObjectRefcount(bb.getInt(16), bb.getInt(20))
    val ts = HLCTimestamp(bb.getLong(24))
    ObjectMetadata(rev, ref, ts)
  }
  
  def bytebufToArray(buf: ByteBuffer): Array[Byte] = {
    val a = new Array[Byte](buf.limit() - buf.position())
    buf.asReadOnlyBuffer().get(a)
    a
  }
}

class RocksDBDataStoreBackend(dbPath:String)(implicit override val executionContext: ExecutionContext) extends DataStoreBackend {
  
  import RocksDBDataStoreBackend._
  
  private[this] val db = new BufferedConsistentRocksDB(dbPath)
  
  private[this] var allocating = Map[UUID, (ObjectMetadata, DataBuffer)]()
  
  override def close(): Future[Unit] = db.close()
  
  // TODO - Freespace checking
  override def haveFreeSpaceForOverwrite(objectId: StoreObjectID, currentDataSize: Int, newDataSize: Int): Boolean = true
  
  override def haveFreeSpaceForAppend(objectId: StoreObjectID, currentDataSize: Int, newDataSize: Int): Boolean = true
  
  override def allocateObject(objectUUID: UUID, metadata: ObjectMetadata, data: DataBuffer): Future[Either[AllocationErrors.Value, Array[Byte]]] = {
    //println(s"Allocating $objectUUID")
    allocating += (objectUUID -> (metadata, data))
    Future.successful(Right(NullArray))
  }
  
  override def deleteObject(objectId: StoreObjectID): Future[Unit] = allocating.get(objectId.objectUUID) match {
    case Some(_) =>
      allocating -= objectId.objectUUID
      Future.successful(())
    case None =>
      Future.sequence(db.delete(metadataKey(objectId)) :: db.delete(dataKey(objectId)) :: Nil).map(_ => ())
  }
  
  override def getObjectMetaData(objectId: StoreObjectID): Future[Either[ObjectReadError, ObjectMetadata]] = {
    allocating.get(objectId.objectUUID) match {
      case Some(t) => Future.successful(Right(t._1))
      case None =>
        db.get(metadataKey(objectId)) map {
          case None => Left(new InvalidLocalPointer)
          case Some(arr) => Right(bytesToMetadata(arr))
        }
    }
  }
  

  override def getObject(objectId: StoreObjectID): Future[Either[ObjectReadError, (ObjectMetadata, DataBuffer)]] = {
    
    allocating.get(objectId.objectUUID) match {
      case Some(t) => 
        //println(s"getObject $objectId (allocating)")
        Future.successful(Right(t))
      case None => for {
        emetadata <- db.get(metadataKey(objectId)) map {
          case None =>
            //println(s"getObject $objectId (NO METADATA)")
            Left(new InvalidLocalPointer)
          case Some(arr) => Right(bytesToMetadata(arr))
        }
        edata <- db.get(dataKey(objectId)) map {
          case None => 
            //println(s"getObject $objectId (NO DATA)")
            Left(new InvalidLocalPointer)
          case Some(arr) => Right(DataBuffer(arr))
        }
      } yield {
        (emetadata, edata) match {
          case (Left(err), Left(_)) => Left(err)
          case (Left(err), Right(_)) => Left(err)
          case (Right(_), Left(err)) => Left(err)
          case (Right(metadata), Right(data)) => Right((metadata, data))
        }
      }
    }
  }
  
  override def putObjectMetaData(objectId: StoreObjectID, metadata: ObjectMetadata): Future[Unit] = {
    //println(s"put ObjectMetaData $objectId")
    allocating.get(objectId.objectUUID) match {
      case Some(t) => putObject(objectId, metadata, t._2)
      case None =>
        db.put(metadataKey(objectId), metadataToBytes(metadata))
    }
  }

  override def putObject(objectId: StoreObjectID, metadata: ObjectMetadata, data: DataBuffer): Future[Unit] = {
    allocating.get(objectId.objectUUID) foreach { _ => allocating -= objectId.objectUUID } 
    //println(s"put Object $objectId")
    Future.sequence(List(
        db.put(metadataKey(objectId), metadataToBytes(metadata)),
        db.put(dataKey(objectId), data.getByteArray()))).map(_ => ())
  }
}