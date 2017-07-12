package com.ibm.aspen.core.transaction

import java.nio.ByteBuffer

class MissingUpdateContent extends LocalUpdateContent {
  def haveDataForUpdateIndex(updateIndex: Int): Boolean = false
  
  def getDataForUpdateIndex(updateIndex: Int): ByteBuffer = throw new Exception("Invalid use of MissingUpdateContent method")
}