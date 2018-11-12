package com.ibm.aspen.core.allocation

import com.ibm.aspen.base.AspenSystem
import com.ibm.aspen.core.objects.keyvalue.{Insert, Key}
import com.ibm.aspen.core.objects.{KeyValueObjectPointer, ObjectPointer, ObjectRevision}
import com.ibm.aspen.core.read.InvalidObject
import com.ibm.aspen.core.transaction.KeyValueUpdate.{KVRequirement, TimestampRequirement}

import scala.concurrent.{ExecutionContext, Future}

sealed abstract class AllocationRevisionGuard {
  val pointer: ObjectPointer
  val requiredRevision: ObjectRevision

  def ensureTransactionCannotCommit(system: AspenSystem)(implicit ec: ExecutionContext): Future[Unit]
}

case class ObjectAllocationRevisionGuard(
                                          pointer: ObjectPointer,
                                          requiredRevision: ObjectRevision
                                        ) extends AllocationRevisionGuard {

  def ensureTransactionCannotCommit(system: AspenSystem)(implicit ec: ExecutionContext): Future[Unit] = {
    system.retryStrategy.retryUntilSuccessful {
      system.readObject(pointer).flatMap { os =>
        if (os.revision == requiredRevision) {
          system.transact { tx =>
            tx.bumpVersion(pointer, requiredRevision)
            Future.unit
          }
        }
        else
          Future.unit
      } recover {
        case _: InvalidObject => ()
      }
    }
  }
}

case class KeyValueAllocationRevisionGuard(
                                            pointer: KeyValueObjectPointer,
                                            key: Key,
                                            requiredRevision: ObjectRevision
                                          ) extends AllocationRevisionGuard {

  def ensureTransactionCannotCommit(system: AspenSystem)(implicit ec: ExecutionContext): Future[Unit] = {
    system.retryStrategy.retryUntilSuccessful {
      system.readObject(pointer).flatMap { kvos =>
        kvos.contents.get(key) match {
          case None => Future.unit
          case Some(v) =>
            if (v.revision == requiredRevision) {
              system.transact { tx =>
                val req = KVRequirement(key, v.timestamp, TimestampRequirement.Equals)
                val op = Insert(key, v.value)
                tx.update(pointer, None, req :: Nil, op :: Nil)
                Future.unit
              }
            }
            else
              Future.unit
        }
      } recover {
        case _: InvalidObject => ()
      }
    }
  }
}