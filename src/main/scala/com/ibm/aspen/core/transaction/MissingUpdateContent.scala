package com.ibm.aspen.core.transaction

class MissingUpdateContent extends LocalUpdateContent {
  def haveDataForUpdateIndex(updateIndex: Int): Boolean = false
}