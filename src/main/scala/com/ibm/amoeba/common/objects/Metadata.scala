package com.ibm.amoeba.common.objects

import com.ibm.amoeba.common.HLCTimestamp

case class Metadata(revision: ObjectRevision,
                    refcount: ObjectRefcount,
                    timestamp: HLCTimestamp)
