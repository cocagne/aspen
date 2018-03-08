package com.ibm.aspen.base

import java.util.UUID

/** TypeFactories serve as the base class for user-extensible types that plug into the Aspen framework
 *  The typeUUID serves to uniquely define the type and allows for common base classes to aggregate
 *  and manage the factories but the details of instance creation are specific to the class of
 *  object being created.
 */
trait TypeFactory {
  val typeUUID: UUID
}