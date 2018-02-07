package com.ibm.aspen.base.keyvalue

import com.ibm.aspen.base.AspenSystem
import com.ibm.aspen.core.objects.keyvalue.Key
import scala.concurrent.Future
import scala.concurrent.ExecutionContext
import com.ibm.aspen.core.objects.KeyValueObjectState
import com.ibm.aspen.base.ObjectReader
import scala.concurrent.Promise
import com.ibm.aspen.core.objects.keyvalue.KeyComparison
import com.ibm.aspen.core.objects.KeyValueObjectPointer
import scala.util.Failure
import scala.util.Success
import com.ibm.aspen.core.objects.ObjectPointer


object KeyValueList {
  
  def fetchContainingNode(
      objectReader: ObjectReader, 
      listPointer: KeyValueListPointer, 
      comparison: KeyComparison,
      key: Key)(implicit ec: ExecutionContext) : Future[KeyValueObjectState] = {
    
    // exit immediately if the requested key is below the minimum range
    if (comparison(key, listPointer.minimum) < 0)
      return Future.failed(new BelowMinimumError(listPointer.minimum, key))
     
    val p = Promise[KeyValueObjectState]()
   
    def scanToContainingNode(pointer: KeyValueObjectPointer): Unit = objectReader.readObject(pointer) onComplete {
      case Failure(err) => p.failure(err)
      case Success(kvos) => 
        if (kvos.keyInRange(key, comparison))
          p.success(kvos)
        else {
          kvos.right match {
            case None => p.failure(new CorruptedLinkedList)
            case Some(arr) => try {
              scanToContainingNode( ObjectPointer.fromArray(arr).asInstanceOf[KeyValueObjectPointer] )
            } catch {
              case err: Throwable => p.failure(new CorruptedLinkedList)
            }
          }
        }
    }
   
    scanToContainingNode(listPointer.pointer)
   
    p.future
  }
}