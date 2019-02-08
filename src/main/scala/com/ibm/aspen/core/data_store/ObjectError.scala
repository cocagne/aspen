package com.ibm.aspen.core.data_store

import com.ibm.aspen.core.objects.ObjectPointer
import com.ibm.aspen.core.objects.ObjectRevision
import com.ibm.aspen.core.objects.ObjectRefcount
import com.ibm.aspen.core.transaction.TransactionDescription
import com.ibm.aspen.core.objects.keyvalue.Key

sealed abstract class ObjectError 

sealed abstract class ObjectReadError extends ObjectError

/** Enumerated values that correspond to Exceptions types. Used as error codes for network messages.
 */
object ObjectReadError extends Enumeration {
  val InvalidLocalPointer = Value("InvalidLocalPointer")
  val ObjectMismatch = Value("ObjectMismatch")
  val CorruptedObject = Value("CorruptedObject")
  
  def apply(err: ObjectReadError): Value = err match {
    case t: InvalidLocalPointer => InvalidLocalPointer
    case t: ObjectMismatch => ObjectMismatch
    case t: CorruptedObject => CorruptedObject
  }
}
  
/** Supplied LocalPointer is not understood by this store or points to an invalid location */
case class InvalidLocalPointer() extends ObjectReadError
  
/** The local pointer is valid but the UUID of the stored object (if any) does not match that of the read request.
 *  
 *  This can occur if the original object is deleted and its storage location has been reassigned to a
 *  new object.
 */
case class ObjectMismatch() extends ObjectReadError

/** Checksum stored with the object does not match the read content. 
 *  
 *  This should be due to media errors 
 */
case class CorruptedObject() extends ObjectReadError


//--------------------------------------------------------------------


