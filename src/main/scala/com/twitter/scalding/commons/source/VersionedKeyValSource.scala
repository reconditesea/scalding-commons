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
import com.twitter.bijection.Bijection
import com.twitter.chill.MeatLocker
import com.twitter.scalding._
import org.apache.hadoop.mapred.JobConf

/**
 * Source used to write key-value pairs as byte arrays into a versioned store.
 * Supports incremental updates via the monoid on V.
 */

class BinaryVersionedSource(path: String, sourceVersion: Option[Long], sinkVersion: Option[Long])
extends VersionedSource(path, sourceVersion, sinkVersion)
with RenameTransformer {
  override val keyField = "key"
  override val valField = "value"

  override def hdfsScheme = HadoopSchemeInstance(new KeyValueByteScheme(new Fields(keyField, valField)))
}

trait KeyValProgression[K,V] extends PipeTransformer {
  def toSource(implicit kCodec: Codec[K, Array[Byte]], vCodec: Codec[V, Array[Byte]]): BinaryVersionedSource
  def incremental(reducers: Int)(implicit monoid: Monoid[V], flowDef: FlowDef, mode: Mode): VersionedKeyValSource[K,V]
  def pack[K1,K2](reducers: Int)(implicit codec: Codec[K,(K1,K2)]): VersionedKeyValSource[K1,Map[K2,List[V]]]
}

object VersionedKeyValSource {

  // Returns a versioned key-value source that transforms kv-pairs to
  // byte-only sequencefiles and back.
  def apply[K,V](path: String, sourceVersion: Option[Long] = None, sinkVersion: Option[Long] = None)
  (implicit codec: Bijection[(K,V),(Array[Byte],Array[Byte])]) =
    new VersionedKeyValSource[K,V](path, sourceVersion, sinkVersion)
}

class VersionedKeyValSource[K,V](val path: String, val sourceVersion: Option[Long], val sinkVersion: Option[Long])
(implicit @transient codec: Bijection[(K,V),(Array[Byte],Array[Byte])]) extends Source with Mappable[(K,V)] {
  import Dsl._

  val keyField = "key"
  val valField = "value"
  val codecBox = MeatLocker(codec)

  override val converter = implicitly[TupleConverter[(K,V)]]

  override def hdfsScheme =
    HadoopSchemeInstance(new KeyValueByteScheme(new Fields(keyField, valField)))

  def getTap(mode: TapMode) = {
    val tap = new VersionedTap(path, hdfsScheme, mode)
    if (mode == TapMode.SOURCE && sourceVersion.isDefined)
      tap.setVersion(sourceVersion.get)
    else if (mode == TapMode.SINK && sinkVersion.isDefined)
      tap.setVersion(sinkVersion.get)
    else
      tap
  }

  override def incremental(numReducers: Int = 1)
  (implicit mv: Monoid[V], flowDef: FlowDef, mode: Mode): VersionedKeyValSource[K,V] = {
    new VersionedKeyValSource[K,V](path, sourceVersion, sinkVersion) with IncrementalTransformer[K,V] {
      override implicit def monoid = mv
      // TODO: KeyVal should accept a supplier function that returns
      // an Option[Pipe]. By default, return None -- at the last
      // minute, in the final step, replace this business with the
      // proper resourceExists shit.
      override def baseSrc = if (resourceExists(mode)) Some(read) else None
      override def reducers = numReducers

  def resourceExists(mode: Mode) =
    mode match {
      case HadoopTest(conf, buffers) => {
        buffers.get(this) map { !_.isEmpty } getOrElse false
      }
      case _ => {
        val conf = new JobConf(mode.asInstanceOf[HadoopMode].jobConf)
        source.resourceExists(conf)
      }
    }
  }

  override def pack[K1,K2](implicit codec: Codec[K,(K1,K2)]) =
    new VersionedKeyValSource[K1,Map[K2,List[V]]](path, sourceVersion, sinkVersion) with PackTransformer[K,K1,K2,V] {
      val safeKeyCodec = new MeatLocker(codec)
      override def innerCodec = safeKeyCodec.get
      override def onRead(pipe: Pipe) = this.onRead(self.onRead(pipe))
      override def onWrite(pipe: Pipe) = self.onWrite(this.onWrite(pipe))
    }
}

// TODO: Move the following traits into Scalding.

  override def transformForRead(pipe: Pipe) = {
    pipe.map((keyField, valField) -> (keyField, valField)) { pair: (Array[Byte],Array[Byte]) =>
      codecBox.get.invert(pair)
    }
  }

  override def transformForWrite(pipe: Pipe) = {
    pipe.mapTo((0,1) -> (keyField, valField)) { pair: (K,V) =>
      codecBox.get.apply(pair)
    }
  }
}

trait IncrementalTransformer[K, V] extends PipeTransformer { self =>
  import Dsl._

  private def appendToken(pipe: Pipe, triplet: Fields, token: Int) =
    pipe.mapTo((0, 1) -> triplet) { pair: (K, V) => pair :+ token }

  implicit def monoid: Monoid[V]
  def baseSrc: Option[Pipe]
  def incrementalReducers: Int = 1

  override def onRead(pipe: Pipe) = super.onRead(pipe)
  override def onWrite(pipe: Pipe) =
    super.onWrite(baseSrc map { src =>
      val fnames = ('key, 'value, 'isNew)
      val oldPairs = appendToken(src, fnames, 0)
      val newPairs = appendToken(pipe, fnames, 1)
      (oldPairs ++ newPairs)
        .groupBy('key) { _.reducers(incrementalReducers).sortBy('isNew).plus[V]('value) }
        .project(('key, 'value))
    } getOrElse pipe)
}

trait PackTransformer[K, K1, K2, V] extends PipeTransformer {
  import Dsl._

  def packReducers: Int
  def innerCodec: Codec[K, (K1, K2)]

  override def onRead(pipe: Pipe) =
    super.onRead(pipe)
      .flatMap((0, 1) -> ('key, 'value)) { pair: (K1, Map[K2, List[V]]) =>
        val k1 = pair._1
        pair._2 flatMap {
          case (k2, lv) =>
            val k = innerCodec.decode((k1, k2))
            lv map { (k, _) }
        }
      }

  override def onWrite(pipe: Pipe) =
    super.onWrite(pipe
      .mapTo((0, 1) -> ('key, 'value)) { pair: (K, V) =>
        val (k1, k2) = innerCodec.encode(pair._1)
        (k1, Map(k2 -> pair._2))
      }
      .groupBy('key) { _.plus[Map[K2, List[V]]]('value) })
}

trait KeyValueCodecTransformer[K, K1, V, V1] extends PipeTransformer {
  import Dsl._

  def keyCodec: Codec[K, K1]
  def valCodec: Codec[V, V1]

  override def onRead(pipe: Pipe) =
    super.onRead(pipe).mapTo((0, 1) -> (0, 1)) { pair: (K1, V1) =>
      (keyCodec.decode(pair._1), valCodec.decode(pair._2))
    }

  override def onWrite(pipe: Pipe) =
    super.onWrite(pipe
      .mapTo((0, 1) -> ('key, 'value)) { pair: (K, V) =>
        (keyCodec.encode(pair._1), valCodec.encode(pair._2))
      })
}

trait RenameTransformer extends PipeTransformer {
  import Dsl._

  def keyField: String
  def valField: String

  override def onRead(pipe: Pipe) = super.onRead(pipe)
  override def onWrite(pipe: Pipe) = super.onWrite(pipe.rename((0, 1) -> (keyField, valField)))
}
