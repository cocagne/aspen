package com.ibm.aspen.base.impl

import org.scalatest.BeforeAndAfter
import java.io.File
import org.scalatest.AsyncFunSuite
import org.scalatest.Matchers

trait TempDirSuiteBase extends AsyncFunSuite with Matchers with BeforeAndAfter {
  var tdir:File = _
  var tdirMgr: TempDirManager = _
  
  before {
    tdirMgr = new TempDirManager
    tdir = tdirMgr.tdir
    preTest()
  }

  after {
    preTempDirDeletion()
    
    tdirMgr.delete()
  }
  
  def preTest(): Unit = {}
  def preTempDirDeletion(): Unit = ()
}