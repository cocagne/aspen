package com.ibm.aspen.base.btree

import java.nio.ByteBuffer
import com.ibm.aspen.core.objects.ObjectPointer

sealed abstract class BTreeOperation

abstract class BTreeContentOperation extends BTreeOperation {
  val key: EncodedKey
}

case class Insert(key:EncodedKey, value:ByteBuffer) extends BTreeContentOperation

case class Delete(key:EncodedKey) extends BTreeContentOperation


abstract class BTreeMaintenanceOperation extends BTreeOperation

case class SetRightPointer(op:ObjectPointer, minimum:EncodedKey) extends BTreeMaintenanceOperation