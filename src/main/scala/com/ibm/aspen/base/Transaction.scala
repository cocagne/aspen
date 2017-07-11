package com.ibm.aspen.base

import java.util.UUID
import scala.concurrent.Future

trait Transaction {
  def uuid: UUID
  
  // How do we receive the Commit/Abort result. And if the tx aborts, what info can we provide to the caller?
  //def result: Future[???]
}