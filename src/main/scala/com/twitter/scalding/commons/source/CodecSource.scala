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

package com.twitter.scalding.source

import cascading.pipe.Pipe
import cascading.scheme.Scheme
import cascading.scheme.hadoop.WritableSequenceFile
import cascading.tap.Tap
import cascading.tap.hadoop.Hfs
import cascading.tuple.Fields
import com.twitter.chill.MeatLocker
import com.twitter.scalding._
import com.twitter.util.Codec
import org.apache.hadoop.io.Writable
import org.apache.hadoop.io.{ ByteWritable, BytesWritable, IntWritable, Text, LongWritable, NullWritable }
import org.apache.hadoop.mapred.{ JobConf, OutputCollector, RecordReader }

/**
 * Source used to write some type T into a WritableSequenceFile using a codec on T
 * for serialization.
 */

object CodecSource {
  def apply[T](path: String)(implicit codec: Codec[T, Array[Byte]], manifest: Manifest[T]) = new CodecSource[T](path)
}

class CodecSource[T] private (path: String)(@transient implicit val codec: Codec[T, Array[Byte]], mf: Manifest[T]) extends Source {
  import Dsl._

  val fieldSym = 'encodedBytes

  val codecBox = new MeatLocker(codec)

  override def hdfsScheme =
    new WritableSequenceFile(new Fields(null, fieldSym.name), classOf[BytesWritable])
      .asInstanceOf[Scheme[JobConf, RecordReader[_, _], OutputCollector[_, _], Array[Object], Array[Object]]]

  lazy val tap = new Hfs(hdfsScheme, path)

  override def createTap(readOrWrite: AccessMode)(implicit mode: Mode): Tap[_, _, _] = {
    mode match {
      case Hdfs(_strict, _config) =>
        readOrWrite match {
          case Read => castHfsTap(tap)
          case Write => castHfsTap(tap)
        }
      case _ => super.createTap(readOrWrite)(mode)
    }
  }

  override def transformForRead(pipe: Pipe) =
    pipe.map((fieldSym) -> (fieldSym)) { codecBox.get.decode(_: Array[Byte]) }

  override def transformForWrite(pipe: Pipe) =
    pipe.mapTo((0) -> (fieldSym)) { codecBox.get.encode(_: T) }
}
