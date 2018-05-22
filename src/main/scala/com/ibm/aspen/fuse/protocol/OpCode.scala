package com.ibm.aspen.fuse.protocol

object OpCode {
  // Message opcodes
  val FUSE_LOOKUP        =  1
  val FUSE_FORGET        =  2  /* no reply */
  val FUSE_GETATTR       =  3
  val FUSE_SETATTR       =  4
  val FUSE_READLINK      =  5
  val FUSE_SYMLINK       =  6
  val FUSE_MKNOD         =  8
  val FUSE_MKDIR         =  9
  val FUSE_UNLINK        = 10
  val FUSE_RMDIR         = 11
  val FUSE_RENAME        = 12
  val FUSE_LINK          = 13
  val FUSE_OPEN          = 14
  val FUSE_READ          = 15
  val FUSE_WRITE         = 16
  val FUSE_STATFS        = 17
  val FUSE_RELEASE       = 18
  val FUSE_FSYNC         = 20
  val FUSE_SETXATTR      = 21
  val FUSE_GETXATTR      = 22
  val FUSE_LISTXATTR     = 23
  val FUSE_REMOVEXATTR   = 24
  val FUSE_FLUSH         = 25
  val FUSE_INIT          = 26
  val FUSE_OPENDIR       = 27
  val FUSE_READDIR       = 28
  val FUSE_RELEASEDIR    = 29
  val FUSE_FSYNCDIR      = 30
  val FUSE_GETLK         = 31
  val FUSE_SETLK         = 32
  val FUSE_SETLKW        = 33
  val FUSE_ACCESS        = 34
  val FUSE_CREATE        = 35
  val FUSE_INTERRUPT     = 36
  val FUSE_BMAP          = 37
  val FUSE_DESTROY       = 38
  val FUSE_IOCTL         = 39
  val FUSE_POLL          = 40
  val FUSE_NOTIFY_REPLY  = 41
  val FUSE_BATCH_FORGET  = 42
  val FUSE_FALLOCATE     = 43
  val FUSE_READDIRPLUS   = 44
  val FUSE_RENAME2       = 45
  val FUSE_LSEEK         = 46
  
  val opcodeNames = Map(
      ( 1 -> "LOOKUP"     ),
      ( 2 -> "FORGET"     ), 
      ( 3 -> "GETATTR"    ), 
      ( 4 -> "SETATTR"    ), 
      ( 5 -> "READLINK"   ), 
      ( 6 -> "SYMLINK"    ), 
      ( 8 -> "MKNOD"      ), 
      ( 9 -> "MKDIR"      ), 
      (10 -> "UNLINK"     ), 
      (11 -> "RMDIR"      ), 
      (12 -> "RENAME"     ), 
      (13 -> "LINK"       ), 
      (14 -> "OPEN"       ), 
      (15 -> "READ"       ), 
      (16 -> "WRITE"      ), 
      (17 -> "STATFS"     ), 
      (18 -> "RELEASE"    ), 
      (20 -> "FSYNC"      ), 
      (21 -> "SETXATTR"   ), 
      (22 -> "GETXATTR"   ), 
      (23 -> "LISTXATTR"  ), 
      (24 -> "REMOVEXATTR"), 
      (25 -> "FLUSH"      ), 
      (26 -> "INIT"       ), 
      (27 -> "OPENDIR"    ), 
      (28 -> "READDIR"    ), 
      (29 -> "RELEASEDIR" ), 
      (30 -> "FSYNCDIR"   ), 
      (31 -> "GETLK"      ), 
      (32 -> "SETLK"      ), 
      (33 -> "SETLKW"     ), 
      (34 -> "ACCESS"     ), 
      (35 -> "CREATE"     ), 
      (36 -> "INTERRUPT"  ), 
      (37 -> "BMAP"       ), 
      (38 -> "DESTROY"    ), 
      (39 -> "IOCTL"      ), 
      (40 -> "POLL"       ), 
      (41 -> "NOTIFY_REPL"),
      (42 -> "BATCH_FORGE"),
      (43 -> "FALLOCATE"  ), 
      (44 -> "READDIRPLUS"), 
      (45 -> "RENAME2"    ), 
      (46 -> "LSEEK"      )) 
}
