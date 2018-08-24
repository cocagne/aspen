package com.ibm.aspen.base.tieredlist

import com.ibm.aspen.base.TypeFactory
import com.ibm.aspen.base.AspenSystem
import com.ibm.aspen.core.DataBuffer

trait TieredKeyValueListNodeAllocaterFactory extends TypeFactory {
  def createNodeAllocater(sys: AspenSystem, db: DataBuffer): SimpleTieredKeyValueListNodeAllocater
}