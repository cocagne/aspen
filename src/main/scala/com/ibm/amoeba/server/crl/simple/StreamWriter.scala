package com.ibm.amoeba.server.crl.simple

import java.nio.ByteBuffer
import java.nio.channels.FileChannel
import java.nio.file.{Path, StandardOpenOption}
import java.util.concurrent.ConcurrentLinkedQueue

object WriterThread:

  sealed abstract class Command

  case class Write(streamId: StreamId,
                   offset: Long,
                   buffers: Array[ByteBuffer]) extends Command

  case class Shutdown() extends Command


class StreamWriter(val maxSizeInBytes: Long,
                   files: List[(StreamId, Path)],
                   onWriteComplete: () => Unit):
  private val queue = new ConcurrentLinkedQueue[WriterThread.Command]()
  private val streams = files.map(t =>
    val channel = FileChannel.open(t._2,
      StandardOpenOption.CREATE,
      StandardOpenOption.DSYNC,
      StandardOpenOption.WRITE)

    if channel.size() < maxSizeInBytes then
      channel.position(maxSizeInBytes-1)
      channel.write(ByteBuffer.allocate(1))
      channel.position(0)

    t._1 -> channel
  ).toMap
  private var exitLoop = false
  private val writerThread = new Thread {
    override def run(): Unit = writeThreadLoop()
  }
  writerThread.start()

  private def writeThreadLoop(): Unit =
    while !exitLoop do
      queue.poll() match
        case w: WriterThread.Write =>
          streams.get(w.streamId).foreach: channel =>
            channel.position(w.offset)
            channel.write(w.buffers)
            onWriteComplete()
            
        case _: WriterThread.Shutdown =>
          streams.valuesIterator.foreach(_.close)
          exitLoop = true
        
  def shutdown(): Unit = queue.add(WriterThread.Shutdown())
  
  def write(streamId: StreamId, offset: Long, buffers: Array[ByteBuffer]): Unit =
    queue.add(WriterThread.Write(streamId, offset, buffers))
