package com.ibm.aspen.base.task

import com.ibm.aspen.core.objects.keyvalue.Key
import scala.concurrent.Future
import com.ibm.aspen.base.Transaction

trait TaskGroupInterface {
  def prepareTask(
      taskType: DurableTaskType, 
      initialState: List[(Key, Array[Byte])])(implicit tx: Transaction): Future[Future[Option[AnyRef]]]
}