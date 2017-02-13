package com.ibm.aspen.core.transaction

import com.ibm.aspen.core.objects.ObjectPointer
import com.ibm.aspen.core.objects.ObjectRefcount

class RefcountUpdate(
    objectPointer: ObjectPointer, 
    requiredRefcount: ObjectRefcount, 
    newRefcount: ObjectRefcount)