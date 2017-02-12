
package com.ibm.aspen

import com.ibm.aspen.core.network.protocol._
import com.google.flatbuffers.FlatBufferBuilder

object QuickTest {
  def main(args: Array[String]): Unit = {
    println("Hello, world2!")
    for (i <- (5-1) to 0 by -1)
      println(s"Index $i")
  }
}
