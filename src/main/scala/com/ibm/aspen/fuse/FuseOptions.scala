package com.ibm.aspen.fuse

/**
 * @param max_readahead Maximum readahead
 * 
 * @param max_background 
 *   Maximum number of pending "background" requests. A
 *   background request is any type of request for which the
 *   total number is not limited by other means. As of kernel
 *   4.8, only two types of requests fall into this category:
 *
 *     1. Read-ahead requests
 *     2. Asynchronous direct I/O requests
 *
 *   Read-ahead requests are generated (if max_readahead is
 *   non-zero) by the kernel to preemptively fill its caches
 *   when it anticipates that userspace will soon read more
 *   data.
 *
 *   Asynchronous direct I/O requests are generated if
 *   FUSE_CAP_ASYNC_DIO is enabled and userspace submits a large
 *   direct I/O request. In this case the kernel will internally
 *   split it up into multiple smaller requests and submit them
 *   to the filesystem concurrently.
 *
 *   Note that the following requests are *not* background
 *   requests: writeback requests (limited by the kernel's
 *   flusher algorithm), regular (i.e., synchronous and
 *   buffered) userspace read/write requests (limited to one per
 *   thread), asynchronous read requests (Linux's io_submit(2)
 *   call actually blocks, so these are also limited to one per
 *   thread).
 *   
 * @param congestion_threshold
 *   Kernel congestion threshold parameter. If the number of pending
 *   background requests exceeds this number, the FUSE kernel module will
 *   mark the filesystem as "congested". This instructs the kernel to
 *   expect that queued requests will take some time to complete, and to
 *   adjust its algorithms accordingly (e.g. by putting a waiting thread
 *   to sleep instead of using a busy-loop).
 *   
 * @param max_write Maximum size of the write buffer
 * 
 * @param time_gran
 *   When FUSE_CAP_WRITEBACK_CACHE is enabled, the kernel is responsible
 *   for updating mtime and ctime when write requests are received. The
 *   updated values are passed to the filesystem with setattr() requests.
 *   However, if the filesystem does not support the full resolution of
 *   the kernel timestamps (nanoseconds), the mtime and ctime values used
 *   by kernel and filesystem will differ (and result in an apparent
 *   change of times after a cache flush).
 *
 *   To prevent this problem, this variable can be used to inform the
 *   kernel about the timestamp granularity supported by the file-system.
 *   The value should be power of 10.  The default is 1, i.e. full
 *   nano-second resolution. Filesystems supporting only second resolution
 *   should set this to 1000000000.
 *   
 * @param capabilities BitMask of desired Capabilities.* constants 
 */
case class FuseOptions(
    val	max_readahead:          Int,
    	val	max_background:         Int, // Short on the wire
    	val	congestion_threshold:   Int, // Short on the wire
    	val	max_write:              Int,
    	val	time_gran:              Int,
    	val	requestedCapabilities:  Int)
