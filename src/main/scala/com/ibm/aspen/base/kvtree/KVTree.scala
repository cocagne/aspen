package com.ibm.aspen.base.kvtree

import com.ibm.aspen.core.objects.ObjectPointer
import com.ibm.aspen.base.AspenSystem
import scala.concurrent.Future
import scala.concurrent.ExecutionContext
import com.ibm.aspen.core.objects.ObjectRevision
import com.ibm.aspen.core.objects.ObjectRefcount
import java.util.UUID
import java.nio.ByteBuffer
import scala.util.Success
import scala.util.Failure
import scala.concurrent.Promise
import com.ibm.aspen.base.Transaction

trait KVTree {
  def treeDescriptionPointer: ObjectPointer
  def system: AspenSystem
  
  def compareKeys(a: Array[Byte], b: Array[Byte]): Int
  
  def get(key: Array[Byte])(implicit ec: ExecutionContext): Option[Array[Byte]]
  
  def put(key: Array[Byte], value: Array[Byte])(implicit ec: ExecutionContext, t: Transaction): Future[Unit]
}

