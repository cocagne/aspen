package com.ibm.aspen.amorfs.error

/** Thrown on failure to read an inode */
case class InvalidInode(number: Long) extends AmorfsError