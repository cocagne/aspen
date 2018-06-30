package com.ibm.aspen.base

import com.ibm.aspen.core.objects.DataObjectPointer
import com.ibm.aspen.core.read.ReadDriver
import scala.concurrent.Future
import com.ibm.aspen.core.objects.KeyValueObjectPointer
import com.ibm.aspen.core.objects.KeyValueObjectState
import com.ibm.aspen.core.objects.DataObjectState
import com.ibm.aspen.core.objects.keyvalue.Key
import com.ibm.aspen.core.objects.keyvalue.KeyOrdering
import com.ibm.aspen.core.objects.ObjectPointer
import com.ibm.aspen.core.objects.ObjectState

trait ObjectReader {
  
  def readObject(pointer: ObjectPointer): Future[ObjectState] = pointer match {
    case dp: DataObjectPointer => readObject(dp)
    case kvp: KeyValueObjectPointer => readObject(kvp)
  }
  
  /** Reads and returns the current state of the object. No caches are used */
  def readObject(pointer:DataObjectPointer, readStrategy: Option[ReadDriver.Factory]): Future[DataObjectState]
  
  /** Reads and returns the current state of the object. No caches are used */
  def readObject(pointer:DataObjectPointer): Future[DataObjectState] = readObject(pointer, None)
  
  /** Reads and returns the current state of the object. No caches are used */
  def readObject(pointer:KeyValueObjectPointer, readStrategy: Option[ReadDriver.Factory]): Future[KeyValueObjectState]
  
  /** Reads and returns the current state of the object. No caches are used */
  def readObject(pointer:KeyValueObjectPointer): Future[KeyValueObjectState] = readObject(pointer, None)
  
  def readSingleKey(pointer: KeyValueObjectPointer, key: Key, comparison: KeyOrdering): Future[KeyValueObjectState]
  
  def readLargestKeyLessThan(pointer: KeyValueObjectPointer, key: Key, comparison: KeyOrdering): Future[KeyValueObjectState]
  
  def readLargestKeyLessThanOrEqualTo(pointer: KeyValueObjectPointer, key: Key, comparison: KeyOrdering): Future[KeyValueObjectState]
  
  def readKeyRange(pointer: KeyValueObjectPointer, minimum: Key, maximum: Key, comparison: KeyOrdering): Future[KeyValueObjectState]
  
  /** Reads and returns a consistent snapshot of the requested objects. 
   *  
   *  Reads are point-in-time queries of object state. To ensure a set of objects are consistent with one another, the goal
   *  is for the read to guarantee that it obtains the state of each object at the time the read was issued. Unlike MVCC 
   *  databases, Aspen does not maintain a history of old values. Consequently, if an object is updated after the point in time
   *  of the snapshot read the previous object state is unrecoverable so the read request will fail. A new read request must then be
   *  issued from a later point in time. For reads requesting multiple objects with high write contention, this may lead to never-ending
   *  retries. A potential future enhancement would be for retries to place their point-in-time reads shortly into the future and increase
   *  the distance into the future with each retry. To ensure consistency when responding to consistent read requests, each store responding to the
   *  request implicitly agrees to vote against all write transactions attempting to modify the object prior to the read timestamp. Without
   *  this guarantee, it would be possible for a read at time 2 to return the state set at time 0 but an outstanding transaction for time 1
   *  that hadn't completed at time 2 could subsequently complete and update the state of the object. In that scenario, the read stamped at
   *  time 2 would incorrectly report the state set at 0 instead of at 1. By ensuring that a majority of stores agree to vote against any
   *  transactions attempting to modify the state before time 2, the potential error condition is avoided. It would be impossible for
   *  the transaction at time 1 to successfully complete.
   *  
   *  Note that this mechanism of having reads interfere with write transactions only applies to objects read in a consistent manner.
   *  This limits the potential for contention to only those sets of objects for which the application requires inter-object consistency.
   *  Objects read individually will not interfere with write transactions.
   *  
   *  Strategy for ensuring consistent cross-object reads:
   *    If a value has been successfully written After the read request was issued, the read fails and must be re-started. If no transactions
   *    are in the process of modifying the objects, the read succeeds. If one or more transactions are in the process of updating the object,
   *    the current locks must be analyzed. If all of the write transactions have timestamps newer than the read transaction, the read is
   *    successful. If a majority of stores agreed to vote against writes with timestamps older than the read timestamp, the read is
   *    successful. Otherwise, the read fails and must be restarted since one of the outstanding transactions may succeed in updating the
   *    state of the object.
   *       
   *  For serializable transaction execution, there are three primary problems that must be solved:
   *  
   *     Read-Write  (RW) – Second operation overwrites a value that was read by the first operation.
   *     Write-Read  (WR) – Second operation reads a value that was written by the first operation.
   *     Write-Write (WW) – Second operation overwrites a value that was written by first operation.
   *     
   *  When fully serializable transactions are required, a transaction must perform a consistent read on all objects it will read
   *  and modify. Even if the object content of the objects-to-be-modified is not required, the read must still be performed in
   *  order to gain the current revision of the object. Write-Write conflicts are avoided by having the transaction require that
   *  the revisions of modified objects exactly match that of read revision. Write-Read conflicts are avoided by way of the consistent
   *  read which ensures that the state of the objects are consistent with one another. Read-Write conflicts are avoided by placing
   *  a revision requirement on all of the objects that were read but are not being modified by the transaction. This version
   *  requirement will cause the transaction to fail if the revision of those objects does not exactly match the revision they
   *  had at the time of the consistent read. 
   *  
   */
  //def readConsistent(pointers: List[ObjectPointer]): Future[(HLCTimestamp, List[ObjectState])]
}