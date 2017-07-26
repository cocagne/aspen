package com.ibm.aspen.base.impl

import org.scalatest.BeforeAndAfter
import java.io.File
import org.scalatest.AsyncFunSuite
import org.scalatest.Matchers

trait TempDirSuiteBase extends AsyncFunSuite with Matchers with BeforeAndAfter {
  var tdir:File = _

  before {
    val tfile = File.createTempFile("scalatest", "TempDirSuiteTest")
    tfile.delete()
    tdir = new File(tfile.toString)
    tdir.mkdir()
  }

  after {
    preTempDirDeletion()
    
    def cleanup(f:File): Unit = {
      if (f.isFile) {
        f.delete()
      }
      else {
        f.listFiles().foreach( cleanup )
        f.delete()
      }
    }
    cleanup(tdir)
  }
  
  def preTempDirDeletion(): Unit = ()
}