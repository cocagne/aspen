package com.ibm.aspen.base.kvtree

import com.ibm.aspen.core.objects.ObjectPointer
import scala.concurrent.Future

trait KVTreeFactory {
    def createTree(treeDefinitionObject: ObjectPointer): Future[KVTree]
}