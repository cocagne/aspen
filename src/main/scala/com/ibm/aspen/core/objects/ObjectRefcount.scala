package com.ibm.aspen.core.objects

case class ObjectRefcount(updateSerial: Int, count: Int) {
  def update(newCount: Int) = ObjectRefcount(updateSerial+1, newCount) 
  
  def increment(): ObjectRefcount = ObjectRefcount(updateSerial+1, count+1)
  def decrement(): ObjectRefcount = ObjectRefcount(updateSerial+1, count-1)
}