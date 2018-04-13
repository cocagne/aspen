package com.ibm.aspen.cumulofs.error

/** Thrown on failure to read an inode */
case class InvalidInode(number: Long) extends CumuloFSError