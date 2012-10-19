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

import cascading.scheme.Scheme
import cascading.scheme.hadoop.WritableSequenceFile
import cascading.scheme.local.{ TextLine => CLTextLine }
import cascading.tuple.Fields
import com.twitter.scalding._
import com.twitter.scalding.Dsl._
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{ FileStatus, FileSystem, Path }
import org.apache.hadoop.io.Writable

trait WritableSequenceScheme extends FileSource {
  def fields: Fields
  def keyClass: Class[_ <: Writable]
  def valueClass: Class[_ <: Writable]
  override def localScheme = { println("This does not work yet"); new CLTextLine }
  override def hdfsScheme = HadoopSchemeInstance(new WritableSequenceFile(fields, keyClass, valueClass))
}

/**
 * Ensures that a _SUCCESS file is present in the Source path.
 */
trait SuccessFileSource extends FileSource {
  override protected def pathIsGood(p: String, conf: Configuration) = {
    val path = new Path(p)
    Option(path.getFileSystem(conf).globStatus(path)).
      map { statuses: Array[FileStatus] =>
        // Must have a file that is called "_SUCCESS"
        statuses.exists { fs: FileStatus =>
          fs.getPath.getName == "_SUCCESS"
        }
      }
      .getOrElse(false)
  }
}
