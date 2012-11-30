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

import backtype.cascading.scheme.KeyValueByteScheme
import backtype.cascading.tap.VersionedTap
import backtype.cascading.tap.VersionedTap.TapMode
import cascading.flow.FlowDef
import cascading.pipe.Pipe
import cascading.scheme.Scheme
import cascading.tap.Tap
import cascading.tuple.Fields
import com.twitter.algebird.Monoid
import com.twitter.chill.MeatLocker
import com.twitter.scalding._
import com.twitter.util.Codec
import org.apache.hadoop.mapred.JobConf

/**
 * Source used to write key-value pairs as byte arrays into a versioned store.
 * Supports incremental updates via the monoid on V.
 */

object VersionedKeyValSource {
  def apply[K,V](path: String, sourceVersion: Option[Long] = None, sinkVersion: Option[Long] = None)
  (implicit keyCodec: Codec[K,Array[Byte]], valCodec: Codec[V,Array[Byte]]) =
    new VersionedKeyValSource[K,V](path, sourceVersion, sinkVersion)
}

class VersionedKeyValSource[K,V](val path: String, val sourceVersion: Option[Long], val sinkVersion: Option[Long])
(implicit @transient keyCodec: Codec[K,Array[Byte]],
 @transient valCodec: Codec[V,Array[Byte]]) extends Source {
  import Dsl._

  val keyField = "key"
  val valField = "value"
  val safeKeyCodec = new MeatLocker(keyCodec)
  val safeValCodec = new MeatLocker(valCodec)

  def incremental(reducers: Int = 1)(implicit monoid: Monoid[V], flowDef: FlowDef, mode: Mode) = {
    val self = this
    new VersionedKeyValSource(path, sourceVersion, sinkVersion)(keyCodec, valCodec) {
      private def appendToken(pipe: Pipe, token: Int) =
        pipe.mapTo((0,1) -> ('key, 'value, 'isNew)) { pair: (K,V) => pair :+ token }

      override def modifyForRead(pipe: Pipe) = self.modifyForRead(pipe)
      override def modifyForWrite(pipe: Pipe) = {
        val outPipe = self.modifyForWrite(pipe)
        if (!resourceExists(mode))
          outPipe
        else {
          val oldPairs = appendToken(read, 0)
          val newPairs = appendToken(outPipe, 1)
          (oldPairs ++ newPairs)
            .groupBy('key) { _.reducers(reducers).sortBy('isNew).plus[V]('value) }
            .project(('key, 'value))
        }
      }
    }
  }

  def pack[K1,K2](implicit monoid: Monoid[V],
                  codec: Codec[K,(K1,K2)],
                  kCodec: Codec[K1,Array[Byte]],
                  vCodec: Codec[Map[K2,V],Array[Byte]]) = {
    val self = this
    new VersionedKeyValSource[K1,Map[K2,V]](path, sourceVersion, sinkVersion)(kCodec, vCodec) {
      val safeInnerCodec = new MeatLocker(codec)
      override def modifyForRead(pipe: Pipe) =
        self.modifyForRead(
          pipe.flatMap((keyField, valField) -> (keyField, valField)) { pair: (K1,Map[K2,V]) =>
            val k1 = pair._1
            pair._2 map { pair: (K2,V) =>
              (safeInnerCodec.get.decode((k1,pair._1)), pair._2)
            }
          }
        )
      override def transformForWrite(pipe: Pipe) =
        self.transformForWrite(pipe)
          .map((0, 1) -> ('key, 'value)) { pair: (K,V) =>
            val (k1,k2) = safeInnerCodec.get.encode(pair._1)
            (k1, Map(k2 -> pair._2))
           }
          .groupBy('key) { _.plus[Map[K2,V]]('value) }
    }
  }

  override def hdfsScheme =
    HadoopSchemeInstance(new KeyValueByteScheme(new Fields(keyField, valField)))

  private def getTap(mode: TapMode) = {
    val tap = new VersionedTap(path, keyField, valField, mode)
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

  override def createTap(readOrWrite: AccessMode)(implicit mode: Mode): Tap[_,_,_] = {
    mode match {
      case Hdfs(_strict, _config) =>
        readOrWrite match {
          case Read  => castHfsTap(source)
          case Write => castHfsTap(sink)
        }
      case _ => super.createTap(readOrWrite)(mode)
    }
  }

  def modifyForRead(pipe: Pipe) = pipe
  def modifyForWrite(pipe: Pipe) = pipe

  override def transformForRead(pipe: Pipe) =
    modifyForRead(
      pipe.map(('key,'value) -> ('key,'value)) { pair: (Array[Byte],Array[Byte]) =>
        (safeKeyCodec.get.decode(pair._1), safeValCodec.get.decode(pair._2))
      }
    )

  override def transformForWrite(pipe: Pipe) =
    modifyForWrite(pipe)
      .mapTo((0,1) -> ('key,'value)) { pair: (K,V) =>
        (safeKeyCodec.get.encode(pair._1), safeValCodec.get.encode(pair._2))
      }

  override def toString =
    "%s path:%s,sourceVersion:%s,sinkVersion:%s".format(getClass(), path, sourceVersion, sinkVersion)

  override def equals(other: Any) =
    if (other.isInstanceOf[VersionedKeyValSource[K, V]]) {
      val otherSrc = other.asInstanceOf[VersionedKeyValSource[K, V]]
      otherSrc.path == path &&
      otherSrc.sourceVersion == sourceVersion &&
      otherSrc.sinkVersion == sinkVersion
    } else {
      false
    }

  override def hashCode = toString.hashCode
}
