package com.ibm.aspen.base.btree

sealed class BTreeError(val msg: String) extends Throwable(msg)

/** Thrown when an fetch attempt is made for a key that is below the minimum value of the
 *  tree node 
 */
class KeyOutOfRange extends BTreeError("KeyOutOfRange")

/** Thrown when the requested node does not exist.
 *
 * This will usually be a result of joins where a node is deleted but the upper branch has not yet
 * been updated to remove the node pointer. This will typically be a non-fatal error.  
 */
class NodeNotFound extends BTreeError("NodeNotFound")