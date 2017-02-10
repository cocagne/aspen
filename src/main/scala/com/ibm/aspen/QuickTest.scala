
package com.ibm.aspen

import com.ibm.aspen.protocol._
import com.google.flatbuffers.FlatBufferBuilder

object QuickTest {
  def main(args: Array[String]): Unit = {
    println("Hello, world2!")
    val builder = new FlatBufferBuilder(1024)
    val or = ObjectRevision.createObjectRevision(builder, 1, 1)
    println(s"Flatbuff thingie: $or")
  }
}
