package com.ibm.amoeba.fs.demo.network

import com.ibm.amoeba.common.store.StoreId
import com.ibm.amoeba.server.StoreManager
import org.apache.logging.log4j.scala.Logging
import org.zeromq.SocketType

import java.nio.file.{Files, Path}

class ZStoreTransferBackend(val transferPort: Int,
                            val net: ZMQNetwork,
                            val storeManager: StoreManager) extends Logging:

  val incomingDir = storeManager.storesDir.resolve("incoming")

  if ! Files.exists(incomingDir) then
    Files.createDirectories(incomingDir)

  private val rthread = new Thread {
    override def run(): Unit = rcvThread()
  }
  rthread.start()

  private def rcvThread(): Unit =
    val incomingSock = net.context.createSocket(SocketType.PULL)

    incomingSock.bind(s"tcp://*:$transferPort")

    var imap: Map[StoreId, Process] = Map()

    while true do
      val sidBytes = incomingSock.recv(0)
      if sidBytes.length == 17 && incomingSock.hasReceiveMore then
        val storeId = StoreId(sidBytes)
        val data = incomingSock.recv(0)

        val ps = imap.get(storeId) match
          case Some(process) => process
          case None =>
            logger.info(s"Beginning transfer of store ${storeId.directoryName}")
            val pb = new ProcessBuilder()
            pb.command("tar", "-xzf", "-")
            pb.directory(incomingDir.toFile)

            val process = pb.start
            imap += (storeId -> process)

            process

        if data.isEmpty then
          ps.getOutputStream.close()
          imap -= storeId
          val storeDir = storeManager.storesDir.resolve(storeId.directoryName)
          Files.move(incomingDir.resolve(storeId.directoryName), storeDir)
          storeManager.loadStoreById(storeId)
          logger.info(s"Completed transfer of store ${storeId.directoryName}")
        else
          ps.getOutputStream.write(data)



