package com.ibm.aspen.core.transaction

class MissingUpdateContent extends LocalUpdateContent {
  def haveDataForUpdateIndex(updateIndex: Int): Boolean = false
  
  def getDataForUpdateIndex(updateIndex: Int): Array[Byte] = throw new Exception("Invalid use of MissingUpdateContent method")
}