package com.ibm.aspen.base.kvtree

import com.ibm.aspen.core.objects.ObjectPointer
import scala.concurrent.Future
import scala.concurrent.ExecutionContext

trait KVTreeFactory {
    def createTree(treeDefinitionObjectPointer: ObjectPointer)(implicit ec: ExecutionContext): Future[KVTree]
}
