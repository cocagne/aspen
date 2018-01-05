package com.ibm.aspen.base.impl.task

import java.util.UUID
import com.ibm.aspen.core.objects.ObjectPointer

case class TaskDefinition(taskTypeUUID: UUID, taskUUID: UUID, taskObject: ObjectPointer)