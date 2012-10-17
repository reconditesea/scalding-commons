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

import backtype.cascading.tap.PailTap
import backtype.hadoop.pail.{Pail, PailStructure}
import cascading.pipe.Pipe
import cascading.scheme.Scheme
import cascading.tap.Tap
import com.twitter.chill.MeatLocker
import com.twitter.scalding._
import com.twitter.util.Codec
import java.util.{ List => JList }
import org.apache.hadoop.mapred.{ JobConf, OutputCollector, RecordReader }
import scala.collection.JavaConverters._

/**
 * The PailSource enables scalding integration with the Pail class in the
 * dfs-datastores library. PailSource allows scalding to sink 1-tuples
 * to subdirectories of a root folder by applying a routing function to
 * each tuple.
 */

// This pail structure pulls in an implicit codec to handle
// serialization. targetFn takes an instance of T and returns a list
// of "path components". Pail joins these components with
// File.separator and sinks the instance of T into the pail at that
// location.
//
// Usual implementations of "validator" will check that the length of
// the supplied list is >= the length f the list returned by
// targetFn. I don't know that there's a good way to do this check at
// compile time.

class CodecPailStructure[T](targetFn: (T) => List[String], validator: (List[String]) => Boolean)
(@transient implicit val codec: Codec[T, Array[Byte]], manifest: Manifest[T])
extends PailStructure[T] {
  val safeCodec = new MeatLocker(codec)
  override def isValidTarget(paths: String*): Boolean = validator(paths.toList)
  override def getTarget(obj: T): JList[String] = targetFn(obj).toList.asJava
  override def serialize(obj: T): Array[Byte] = safeCodec.get.encode(obj)
  override def deserialize(bytes: Array[Byte]): T = safeCodec.get.decode(bytes)
  override val getType = manifest.erasure.asInstanceOf[Class[T]]
}

object PailSource {
  // Generic version of PailSource accepts a PailStructure.
  def apply[T](rootPath: String, structure: PailStructure[T]) =
    new PailSource(rootPath, structure, null)

  def apply[T](rootPath: String, structure: PailStructure[T], subPaths: Array[List[String]]) =
    new PailSource(rootPath, structure, subPaths)

  // A PailSource can also build its structure on the fly from a
  // couple of functions.
  def apply[T](rootPath: String, targetFn: (T) => List[String], validator: (List[String]) => Boolean)
  (implicit codec: Codec[T,Array[Byte]], manifest: Manifest[T]) =
    new PailSource(rootPath, new CodecPailStructure[T](targetFn, validator), null)

  def apply[T](rootPath: String, targetFn: (T) => List[String], validator: (List[String]) => Boolean, subPaths: Array[List[String]] = null)
  (implicit codec: Codec[T,Array[Byte]], manifest: Manifest[T]) =
    new PailSource(rootPath, new CodecPailStructure[T](targetFn, validator), subPaths)
}

class PailSource[T] private (rootPath: String, structure: PailStructure[T], subPaths: Array[List[String]] = null)
extends Source with Mappable[T] {
  import Dsl._

  val fieldName = "pailItem"

  lazy val getTap = {
    val spec = PailTap.makeSpec(null, structure)
    val opts = new PailTap.PailTapOptions(spec, fieldName, subPaths map { _.asJava }, null)
    new PailTap(rootPath, opts)
  }

  override def hdfsScheme = getTap.getScheme
    .asInstanceOf[Scheme[JobConf, RecordReader[_, _], OutputCollector[_, _], Array[Object], Array[Object]]]

  override def createTap(readOrWrite: AccessMode)(implicit mode: Mode): Tap[_, _, _] = {
    val tap = castHfsTap(getTap)

    mode match {
      case Hdfs(strict, config) =>
        readOrWrite match {
          case Read  => tap
          case Write => tap
        }
      case _ => super.createTap(readOrWrite)(mode)
    }
  }

  // Create pail at rootPath if it doesn't exist, no-op otherwise.
  override def transformForWrite(pipe: Pipe) = {
    Pail.create(rootPath, structure, false)
    pipe.rename((0) -> 'pailItem)
  }

}
