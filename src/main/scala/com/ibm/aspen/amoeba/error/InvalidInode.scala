package com.ibm.aspen.amoeba.error

/** Thrown on failure to read an inode */
case class InvalidInode(number: Long) extends AmoebaError