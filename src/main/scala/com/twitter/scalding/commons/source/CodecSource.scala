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
import cascading.scheme.Scheme
import cascading.scheme.hadoop.WritableSequenceFile
import cascading.tuple.Fields
import com.twitter.chill.MeatLocker
import com.twitter.scalding._
import com.twitter.util.{ Bijection, Codec }
import java.util.Arrays
import org.apache.hadoop.io.BytesWritable
import org.apache.hadoop.mapred.{ JobConf, OutputCollector, RecordReader }

/**
 * Source used to write some type T into a WritableSequenceFile using a codec on T
 * for serialization.
 */

object BytesWritableCodec extends Codec[Array[Byte], BytesWritable] {
  override def encode(b: Array[Byte]) = new BytesWritable(b)
  override def decode(w: BytesWritable) = Arrays.copyOfRange(w.getBytes, 0, w.getLength)
}

object CodecSource {
  def apply[T](paths: String*)(implicit codec: Bijection[T, Array[Byte]]) = new CodecSource[T](paths)
}

class CodecSource[T] private (val hdfsPaths: Seq[String])(@transient implicit val codec: Bijection[T, Array[Byte]]) extends FileSource {
  import Dsl._

  val fieldSym = 'encodedBytes
  val codecBox = new MeatLocker(codec andThen BytesWritableCodec)

  override def localPath = sys.error("Local mode not yet supported.")

  override def hdfsScheme =
    new WritableSequenceFile(new Fields(fieldSym.name), classOf[BytesWritable])
      .asInstanceOf[Scheme[JobConf, RecordReader[_, _], OutputCollector[_, _], Array[Object], Array[Object]]]

  override def transformForRead(pipe: Pipe) =
    pipe.map((fieldSym) -> (fieldSym)) { codecBox.get.invert(_: BytesWritable) }

  override def transformForWrite(pipe: Pipe) =
    pipe.mapTo((0) -> (fieldSym)) { codecBox.get.apply(_: T) }
}
