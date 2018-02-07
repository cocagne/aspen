package com.ibm.aspen.base

import com.ibm.aspen.core.objects.DataObjectPointer
import com.ibm.aspen.core.read.ReadDriver
import scala.concurrent.Future
import com.ibm.aspen.core.objects.KeyValueObjectPointer
import com.ibm.aspen.core.objects.KeyValueObjectState
import com.ibm.aspen.core.objects.DataObjectState
import com.ibm.aspen.core.objects.keyvalue.Key
import com.ibm.aspen.core.objects.keyvalue.KeyComparison

trait ObjectReader {
  /** Reads and returns the current state of the object. No caches are used */
  def readObject(pointer:DataObjectPointer, readStrategy: Option[ReadDriver.Factory]): Future[DataObjectState]
  
  /** Reads and returns the current state of the object. No caches are used */
  def readObject(pointer:DataObjectPointer): Future[DataObjectState] = readObject(pointer, None)
  
  /** Reads and returns the current state of the object. No caches are used */
  def readObject(pointer:KeyValueObjectPointer, readStrategy: Option[ReadDriver.Factory]): Future[KeyValueObjectState]
  
  /** Reads and returns the current state of the object. No caches are used */
  def readObject(pointer:KeyValueObjectPointer): Future[KeyValueObjectState] = readObject(pointer, None)
  
  def readSingleKey(pointer: KeyValueObjectPointer, key: Key, comparison: KeyComparison): Future[KeyValueObjectState]
  
  def readLargestKeyLessThan(pointer: KeyValueObjectPointer, key: Key, comparison: KeyComparison): Future[KeyValueObjectState]
  
  def readKeyRange(pointer: KeyValueObjectPointer, minimum: Key, maximum: Key, comparison: KeyComparison): Future[KeyValueObjectState]
}