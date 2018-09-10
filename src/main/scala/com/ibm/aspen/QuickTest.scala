
package com.ibm.aspen

import com.ibm.aspen.core.network.protocol._
import com.google.flatbuffers.FlatBufferBuilder

object QuickTest {
  sealed abstract class Foo
  class Bar extends Foo
  class Baz extends Foo

  class A(val foo: Foo)

  class B(override val foo: Bar) extends A(foo)

  def main(args: Array[String]): Unit = {
    println("Hello, world2!")
    for (i <- 0 until 5)
      println(s"Index $i")
  }
}
