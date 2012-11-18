/*
Copyright 2012 Twitter, Inc.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package com.twitter.scalding.commons.source

import cascading.pipe.Pipe
import cascading.tuple.Fields
import com.twitter.chill.MeatLocker
import com.twitter.elephantbird.cascading2.scheme.LzoByteArrayScheme
import com.twitter.scalding._
import com.twitter.util.Bijection

/**
 * Source used to write some type T into an LZO-compressed SequenceFile using a
 * codec on T for serialization.
 */

object LzoCodecSource {
  def apply[T](paths: String*)(implicit codec: Bijection[T, Array[Byte]]) = new LzoCodecSource[T](paths)
}

class LzoCodecSource[T] private (val hdfsPaths: Seq[String])(@transient implicit val codec: Bijection[T, Array[Byte]])
  extends FileSource
  with Mappable[T] {
  import Dsl._
  val codecBox = new MeatLocker(codec)

  override val converter = Dsl.singleConverter[T]
  override def localPath = sys.error("Local mode not yet supported.")

  override def hdfsScheme = HadoopSchemeInstance(new LzoByteArrayScheme)

  override def transformForRead(pipe: Pipe) =
    pipe.map((0) -> (0)) { codecBox.get.invert(_: Array[Byte]) }

  override def transformForWrite(pipe: Pipe) =
    pipe.mapTo((0) -> (0)) { codecBox.get.apply(_: T) }
}
