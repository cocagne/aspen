package com.ibm.aspen.base.impl

import org.scalatest.AsyncFunSuite
import org.scalatest.Matchers
import org.scalatest.BeforeAndAfter
import java.io.File
import scala.concurrent.Await
import scala.concurrent.duration._
import java.util.Arrays
import scala.concurrent.ExecutionContext

object BufferedConsistentRocksDBSuite {
  val awaitDuration = Duration(100, MILLISECONDS)
}
class BufferedConsistentRocksDBSuite extends TempDirSuiteBase {
  import BufferedConsistentRocksDBSuite._
  
  var db: BufferedConsistentRocksDB = null
  
  override def preTempDirDeletion(): Unit = if (db!=null) db.close()
  
  test("Test foreach") {
    
    val tpath = new File(tdir, "dbdir").getAbsolutePath
    
    db = new BufferedConsistentRocksDB(tpath)(ExecutionContext.Implicits.global)
    
    val foo = "foo".getBytes()
    val bar = "bar".getBytes()
    val baz = "baz".getBytes()
    
    val f1 = db.put(foo, bar)
    val f2 = db.put(bar, foo)
    val f3 = db.put(baz, bar)
    
    Await.result(f3, awaitDuration)
    
    Await.result(db.delete(bar), awaitDuration)
    
    Await.result(db.close(), Duration(10000, MILLISECONDS))
    
    db = new BufferedConsistentRocksDB(tpath)(ExecutionContext.Implicits.global)
    
    var kvl = List[(Array[Byte], Array[Byte])]()
    
    def fn(key:Array[Byte], value: Array[Byte]) = kvl = (key,value) :: kvl
    
    Await.result(db.foreach(fn), awaitDuration)
    
    kvl.size should be (2)
    
    Arrays.equals(kvl.head._1, foo) should be (true)
    Arrays.equals(kvl.head._2, bar) should be (true)
    Arrays.equals(kvl.tail.head._1, baz) should be (true)
    Arrays.equals(kvl.tail.head._2, bar) should be (true)
  }
  
  test("Test multiple puts") {
    
    val tpath = new File(tdir, "dbdir").getAbsolutePath
    
    db = new BufferedConsistentRocksDB(tpath)(ExecutionContext.Implicits.global)
    
    val foo = "foo".getBytes()
    val bar = "bar".getBytes()
    val baz = "baz".getBytes()
    
    val f1 = db.put(foo, bar)
    val f2 = db.put(bar, foo)
    val f3 = db.put(baz, bar)
    
    Await.result(f3, awaitDuration)
    
    f1.isCompleted should be (true)
    f2.isCompleted should be (true)
    
    val v = Await.result(db.get(foo), awaitDuration)
    v.isDefined should be (true)
    Arrays.equals(v.get, bar) should be (true)
    
    val v2 = Await.result(db.get(baz), awaitDuration)
    v2.isDefined should be (true)
    Arrays.equals(v2.get, bar) should be (true)
  }
  
  test("Test put get") {
    
    val tpath = new File(tdir, "dbdir").getAbsolutePath
    
    db = new BufferedConsistentRocksDB(tpath)(ExecutionContext.Implicits.global)
    
    val foo = "foo".getBytes()
    val bar = "bar".getBytes()
    
    Await.result(db.put(foo, bar), awaitDuration)
    
    val v = Await.result(db.get(foo), awaitDuration)
    
    v.isDefined should be (true)
    
    Arrays.equals(v.get, bar) should be (true)
  }
  
  test("Test delete") {
    
    val tpath = new File(tdir, "dbdir").getAbsolutePath
    
    db = new BufferedConsistentRocksDB(tpath)(ExecutionContext.Implicits.global)
    
    val foo = "foo".getBytes()
    val bar = "bar".getBytes()
    
    Await.result(db.put(foo, bar), awaitDuration)
    
    val v = Await.result(db.get(foo), awaitDuration)
    
    v.isDefined should be (true)
    
    Arrays.equals(v.get, bar) should be (true)
    
    Await.result(db.delete(foo), awaitDuration)
    
    val v2 = Await.result(db.get(foo), awaitDuration)
    
    v2.isDefined should be (false)
  }
}