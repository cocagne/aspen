package com.ibm.aspen.base.kvtree

import com.ibm.aspen.core.objects.ObjectPointer
import scala.concurrent.Future
import scala.concurrent.ExecutionContext
import com.ibm.aspen.core.objects.DataObjectPointer

trait KVTreeFactory {
    def createTree(treeDefinitionObjectPointer: DataObjectPointer)(implicit ec: ExecutionContext): Future[KVTree]
}
