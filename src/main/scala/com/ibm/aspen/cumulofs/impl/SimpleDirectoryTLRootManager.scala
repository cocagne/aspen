package com.ibm.aspen.cumulofs.impl

import java.util.UUID

import com.ibm.aspen.base.tieredlist._
import com.ibm.aspen.base.{AspenSystem, Transaction}
import com.ibm.aspen.core.DataBuffer
import com.ibm.aspen.core.objects.DataObjectPointer
import com.ibm.aspen.core.objects.keyvalue.{Insert, Key}
import com.ibm.aspen.cumulofs.DirectoryInode

import scala.concurrent.{ExecutionContext, Future}

class SimpleDirectoryTLRootManager(val system: AspenSystem,
                                   val inodePointer: DataObjectPointer,
                                   initialRoot: TieredKeyValueListRoot) extends TieredKeyValueListMutableRootManager {

  val typeUUID: UUID = SimpleDirectoryTLRootManager.typeUUID

  private[this] var troot = initialRoot

  def root: TieredKeyValueListRoot = synchronized { troot }

  def refresh(implicit ec: ExecutionContext): Future[TieredKeyValueListRoot] = {
    system.readObject(inodePointer).map{ dos =>
      val inode = DirectoryInode(dos.data)

      inode.ocontents match {
        case None => throw new InvalidRoot // must have been deleted. tree no longer exists

        case Some(tkvlr) => synchronized {
          troot = tkvlr
          tkvlr
        }
      }
    }
  }

  def serialize(): Array[Byte] = inodePointer.toArray

  def prepareRootUpdate(newRootTier: Int,
                        allocater: TieredKeyValueListNodeAllocater,
                        inserted: List[KeyValueListPointer])(implicit tx: Transaction, ec: ExecutionContext): Future[Unit] = {

    tx.note(s"Updating SimpleDirectoryTKVL root with new tier $newRootTier. Child nodes: ${inserted.map(_.pointer.uuid)}")

    val falloc = allocater.tierNodeAllocater(newRootTier)
    val iops = Insert(Key.AbsoluteMinimum, root.rootNode.toArray) :: inserted.map(p => Insert(p.minimum, p.pointer.toArray))

    for {
      dos <- system.readObject(inodePointer)
      allocater <- falloc

      inode = DirectoryInode(dos.data)

      newRootPointer <- allocater.allocateKeyValueObject(dos.pointer, dos.revision, iops)
    } yield {
      inode.ocontents match {
        case None => throw new InvalidRoot

        case Some(currentRoot) =>
          if (currentRoot.topTier >= newRootTier)
            throw new TierAlreadyCreated

          val newRoot = currentRoot.copy(topTier = newRootTier, rootNode = newRootPointer)

          val newInode = inode.setContentTree(Some(newRoot))

          tx.overwrite(inodePointer, dos.revision, newInode.toDataBuffer)

          tx.result.foreach { _ => synchronized {
            troot = newRoot
          }}
      }
    }
  }

  def prepareRootDeletion()(implicit tx: Transaction, ec: ExecutionContext): Future[Unit] = {
    tx.note("Preparing deletion of SimpleDirectory TKVL")

    system.readObject(inodePointer).map{ dos =>
      val inode = DirectoryInode(dos.data)

      inode.ocontents.foreach { _ =>
        val newInode = inode.setContentTree(None)

        tx.overwrite(inodePointer, dos.revision, newInode.toDataBuffer)
      }
    }
  }
}

object SimpleDirectoryTLRootManager extends TieredKeyValueListMutableRootManagerFactory {
  val typeUUID: UUID = UUID.fromString("EB11245E-4931-451E-A65F-A316BEFFCA5B")

  def createMutableRootManager(system: AspenSystem,
                               serializedRootManager: DataBuffer)(implicit ec: ExecutionContext): Future[TieredKeyValueListMutableRootManager] = {

    val inodePointer = DataObjectPointer(serializedRootManager)

    system.readObject(inodePointer) map { dos =>
      val inode = DirectoryInode(dos.data)
      inode.ocontents match {
        case None => throw new InvalidRoot

        case Some(initialRoot) => new SimpleDirectoryTLRootManager(system, inodePointer, initialRoot)
      }
    }
  }
}
