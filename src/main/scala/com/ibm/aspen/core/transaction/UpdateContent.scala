package com.ibm.aspen.core.transaction

import com.ibm.aspen.core.DataBuffer

/** Holds the content for an object update and identifies which update within the transaction description's
 *  dataUpdates list the content applies to. 
 */
case class UpdateContent(
    dataUpdateIndex: Int,
    content: DataBuffer)