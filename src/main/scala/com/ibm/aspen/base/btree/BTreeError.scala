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
class NodeNotFound[Key <: Ordered[Key]](val nodePointer: BTreeNode.NodePointer[Key]) extends BTreeError("NodeNotFound")


/** Thrown if none of embedded pointers in a node may be used to fetch a lower node for the specified key
 *  
 *  This can occur during joins where lower pointers may point deleted nodes. When this occurs, we need
 *  to back up and re-try the search from the next node up in the tree. A case in which this error could
 *  propagate up to the user is requests for keys below the minimum of the left-most tier zero node.
 */
class UnreachableKey extends BTreeError("UnreachableKey")