package com.ibm.amoeba.fs.impl.simple

import java.util.UUID

import com.ibm.amoeba.client.{AmoebaClient, KeyValueObjectState, Transaction}
import com.ibm.amoeba.common.objects.{Insert, Key, ObjectRefcount, ObjectRevision}
import com.ibm.amoeba.common.transaction.KeyValueUpdate.KeyRevision
import com.ibm.amoeba.compute.{DurableTask, DurableTaskPointer, DurableTaskType, TaskExecutor}
import com.ibm.amoeba.common.util.{byte2uuid, uuid2byte}
import com.ibm.amoeba.fs.{FileSystem, Inode, InodePointer}

import scala.concurrent.{ExecutionContext, Future, Promise}
import scala.util.{Failure, Success}

object UnlinkFileTask extends DurableTaskType {
  val typeUUID: UUID = UUID.fromString("B02539DC-3AE1-4E50-B52B-A5EFA6B5B330")

  private val FileSystemUUIDKey = Key(1)
  private val InodePointerKey   = Key(2)
  private val StepKey           = Key(3)

  def createTask(client: AmoebaClient,
                 pointer: DurableTaskPointer,
                 revision: ObjectRevision,
                 state: Map[Key, KeyValueObjectState.ValueState]): DurableTask = {

    val fsUUID = byte2uuid(state(FileSystemUUIDKey).value.bytes)
    val ptr = InodePointer(state(InodePointerKey).value.bytes)

    val fs = FileSystem.getRegisteredFileSystem(fsUUID).get

    new UnlinkFileTask(pointer, fs, ptr, state)
  }

  def prepareTask(fileSystem: FileSystem,
                  taskExecutor: TaskExecutor,
                  inodePointer: InodePointer)(implicit tx: Transaction): Future[Future[Option[AnyRef]]] = {
    val istate = List((FileSystemUUIDKey -> uuid2byte(fileSystem.uuid)),
      (InodePointerKey -> inodePointer.toArray))
    taskExecutor.prepareTask(this, istate)
  }
}

class UnlinkFileTask(val taskPointer: DurableTaskPointer,
                     val fs: FileSystem,
                     val iptr: InodePointer,
                     initialState: Map[Key, KeyValueObjectState.ValueState]) extends DurableTask {

  import UnlinkFileTask._

  implicit val ec: ExecutionContext = fs.executionContext

  private val promise = Promise[Option[AnyRef]]()

  def completed: Future[Option[AnyRef]] = promise.future

  doNextStep()

  def doNextStep(): Unit = {
    for {
      kvos <- fs.client.read(taskPointer.kvPointer)
      (inode, revision) <- fs.readInode(iptr)
      step = kvos.contents(StepKey)
    } yield {
      step.value.bytes(0) match {
        case 0 => decrementLinkCount(step.revision, inode, revision) onComplete {
          case Failure(_) => doNextStep()
          case Success(_) => doNextStep()
        }

        case 1 => checkForDeletion(step.revision, inode, revision) onComplete {
          case Failure(_) => doNextStep()
          case Success(_) => doNextStep()
        }

        case _ => synchronized {
          if (! promise.isCompleted) {
            promise.success(None)
          }
        }
      }
    }
  }

  def decrementLinkCount(stepRevision: ObjectRevision,
                         inode: Inode,
                         revision: ObjectRevision): Future[Unit] = {

    val nextStep = Insert(StepKey, Array[Byte](1)) :: Nil
    val newLinks = inode.links - 1
    val tx = fs.client.newTransaction()
    tx.overwrite(iptr.pointer, revision, inode.update(links=Some(newLinks)).toArray)
    tx.update(taskPointer.kvPointer, None, None, KeyRevision(StepKey, stepRevision) :: Nil, nextStep)
    tx.commit().map(_=>())
  }

  def checkForDeletion(stepRevision: ObjectRevision,
                       inode: Inode,
                       revision: ObjectRevision): Future[Unit] = {
    val nextStep = Insert(StepKey, Array[Byte](2)) :: Nil
    implicit val tx: Transaction = fs.client.newTransaction()

    tx.update(taskPointer.kvPointer, None, None, KeyRevision(StepKey, stepRevision) :: Nil, nextStep)

    if (inode.links == 0) {
      // TODO delete file content
      fs.inodeTable.delete(iptr)
      tx.setRefcount(iptr.pointer, ObjectRefcount(0,1), ObjectRefcount(1,0))
    }

    tx.commit().map(_=>())
  }
}
