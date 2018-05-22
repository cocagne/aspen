package com.ibm.aspen.fuse
/*
 struct fuse_attr {
	uint64_t	ino;
	uint64_t	size;
	uint64_t	blocks;
	uint64_t	atime;
	uint64_t	mtime;
	uint64_t	ctime;
	uint32_t	atimensec;
	uint32_t	mtimensec;
	uint32_t	ctimensec;
	uint32_t	mode;
	uint32_t	nlink;
	uint32_t	uid;
	uint32_t	gid;
	uint32_t	rdev;
	uint32_t	blksize;
	uint32_t	padding;
};
 */
case class Stat(
    inode:     Long, 
    size:      Long, 
    blocks:    Long, 
    atime:     Long,
    mtime:     Long,
    ctime:     Long,
    atimensec: Int,
    mtimensec: Int,
    ctimensec: Int,
    mode:      Int,
    nlink:     Int,
    uid:       Int,
    gid:       Int,
    rdev:      Int,
    blksize:   Int)
