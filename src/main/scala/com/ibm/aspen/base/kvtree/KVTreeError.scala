package com.ibm.aspen.base.kvtree

import com.ibm.aspen.core.objects.ObjectPointer

sealed class KVTreeError(val msg: String) extends Throwable(msg)

/** Thrown when an fetch attempt is made for a key that is below the minimum value of the
 *  tree node 
 */
class KeyOutOfRange extends KVTreeError("KeyOutOfRange")

/** Thrown if fetchRightNode is called on a node that lacks a rightNode pointer
 * 
 */
class NoRightNode extends KVTreeError("NoRightNode")


/** Thrown when the requested node does not exist.
 *
 * This will usually be a result of joins where a node is deleted but the upper branch has not yet
 * been updated to remove the node pointer. This will typically be a non-fatal error.  
 */
class NodeNotFound(val objectPointer:ObjectPointer, val minimum:Array[Byte]) extends KVTreeError("NodeNotFound")


/** Thrown if none of embedded pointers in a node may be used to fetch a lower node for the specified key
 *  
 *  This can occur during joins where lower pointers may point deleted nodes. When this occurs, we need
 *  to back up and re-try the search from the next node up in the tree. A case in which this error could
 *  propagate up to the user is requests for keys below the minimum of the left-most tier zero node.
 */
class UnreachableKey extends KVTreeError("UnreachableKey")


/** Thrown when an attempt is made to fetch a node to the left of left-most node in a tier
 */
class NoLeftNode extends KVTreeError("NoLeftNode")


/** Thrown if the target node is not found during scan for the previous node
 */
class MissingRightScanTarget extends KVTreeError("MissingRightScanTarget")


/** Thrown if an insert exceeds the size of a single node */
class InsertOverflow extends KVTreeError("InsertOverflow")


/** Thrown if a key or value is too large to encode */
class EncodingSizeError extends KVTreeError("EncodingSizeError")