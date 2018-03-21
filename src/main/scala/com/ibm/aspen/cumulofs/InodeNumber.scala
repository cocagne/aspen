package com.ibm.aspen.cumulofs

final class InodeNumber(val number: Long) extends AnyVal

object InodeNumber {
  def apply(number: Long): InodeNumber = new InodeNumber(number)
}