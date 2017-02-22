
package com.ibm.aspen

import com.ibm.aspen.core.network.protocol._
import com.google.flatbuffers.FlatBufferBuilder

object QuickTest {
  def main(args: Array[String]): Unit = {
    println("Hello, world2!")
    for (i <- 0 until 5)
      println(s"Index $i")
  }
}
