package com.ibm.aspen.core.transaction

import java.nio.ByteBuffer

/** Contains the data associated with DataUpdates in the TransactionDescription
 * 
 * The contained updates are specific to this node only. Not all DataUpdate entries in the
 * transaction description will apply to every node so the number of contained update entries
 * will not always match the length of the updates list.
 */
trait LocalUpdateContent {
  
  def haveDataForUpdateIndex(updateIndex: Int): Boolean
  
  def getDataForUpdateIndex(updateIndex: Int): ByteBuffer
}