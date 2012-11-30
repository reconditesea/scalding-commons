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

import backtype.cascading.tap.VersionedTap
import backtype.cascading.tap.VersionedTap.TapMode
import cascading.pipe.Pipe
import cascading.tap.Tap
import com.twitter.scalding._
import org.apache.hadoop.mapred.JobConf

class VersionedSource(val path: String, val sourceVersion: Option[Long], val sinkVersion: Option[Long])
  extends Source
  with PipeTransformer {
  import Dsl._

  private def getTap(mode: TapMode) = {
    val tap = new VersionedTap(path, hdfsScheme, mode)
    if (mode == TapMode.SOURCE && sourceVersion.isDefined)
      tap.setVersion(sourceVersion.get)
    else if (mode == TapMode.SINK && sinkVersion.isDefined)
      tap.setVersion(sinkVersion.get)
    else
      tap
  }

  lazy val source = getTap(TapMode.SOURCE)
  lazy val sink = getTap(TapMode.SINK)

  def resourceExists(mode: Mode): Boolean =
    mode match {
      case HadoopTest(conf, buffers) => {
        buffers.get(this) map { !_.isEmpty } getOrElse false
      }
      case _ => {
        val conf = new JobConf(mode.asInstanceOf[HadoopMode].jobConf)
        !source.resourceExists(conf)
      }
    }

  override def createTap(readOrWrite: AccessMode)(implicit mode: Mode): Tap[_, _, _] = {
    mode match {
      case Hdfs(_strict, _config) =>
        readOrWrite match {
          case Read => castHfsTap(source)
          case Write => castHfsTap(sink)
        }
      case _ => super.createTap(readOrWrite)(mode)
    }
  }

  override def transformForRead(pipe: Pipe) = onRead(pipe)
  override def transformForWrite(pipe: Pipe) = onWrite(pipe)

  override def toString =
    "%s path:%s,sourceVersion:%s,sinkVersion:%s".format(getClass(), path, sourceVersion, sinkVersion)

  override def equals(other: Any) =
    if (other.isInstanceOf[VersionedSource]) {
      val otherSrc = other.asInstanceOf[VersionedSource]
      otherSrc.path == path &&
        otherSrc.sourceVersion == sourceVersion &&
        otherSrc.sinkVersion == sinkVersion
    } else {
      false
    }

  override def hashCode = toString.hashCode
}
