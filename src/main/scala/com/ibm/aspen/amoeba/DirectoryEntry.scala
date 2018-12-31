package com.ibm.aspen.amoeba

import java.nio.charset.StandardCharsets

import com.ibm.aspen.core.objects.keyvalue.{Key, Value}

case class DirectoryEntry(name: String, pointer: InodePointer) {
  def key: Key = Key(name)
  
  def value: Array[Byte] = pointer.toArray
}

object DirectoryEntry {
  def apply(v: Value): DirectoryEntry = {
    new DirectoryEntry(new String(v.key.bytes, StandardCharsets.UTF_8), InodePointer(v.value))
  }
}