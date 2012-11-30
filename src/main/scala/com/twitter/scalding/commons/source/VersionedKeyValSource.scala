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

/*
object VersionedKeyValSource {
  def apply[K, V](path: String, sourceVersion: Option[Long] = None, sinkVersion: Option[Long] = None)(implicit keyCodec: Codec[K, Array[Byte]], valCodec: Codec[V, Array[Byte]]) =
    new VersionedKeyValSource[K, V](path, sourceVersion, sinkVersion)
}

class PackedVersionedKeyValSource[K, K1, K2, V](path: String, sourceVersion: Option[Long], sinkVersion: Option[Long])(implicit keyCodec: Codec[K1, Array[Byte]], valCodec: Codec[Map[K2, List[V]], Array[Byte]])
  extends VersionedKeyValSource[K1, Map[K2, List[V]]](path, sourceVersion, sinkVersion)
  with Mappable[(K, V)]
*/

class BinaryVersionedSource(path: String, sourceVersion: Option[Long], sinkVersion: Option[Long])
extends VersionedSource(path, sourceVersion, sinkVersion)
with RenameTransformer { self =>
  override val keyField = "key"
  override val valField = "value"

  override def hdfsScheme = HadoopSchemeInstance(new KeyValueByteScheme(new Fields(keyField, valField)))
}

trait KeyValThing[K,V] {
  def toSource(implicit kCodec: Codec[K, Array[Byte]], vCodec: Codec[V, Array[Byte]]): BinaryVersionedSource
  def increment(reducers: Int = 1)(implicit monoid: Monoid[V], flowDef: FlowDef, mode: Mode): KeyValThing[K,V]
  def pack[K1,K2](implicit codec: Codec[K,(K1,K2)]): KeyValThing[K1,Map[K2,List[V]]]
}

class KeyVal[K,V](path: String, sourceVersion: Option[Long], sinkVersion: Option[Long]) extends KeyValThing[K,V] {
  def toSource(implicit kCodec: Codec[K, Array[Byte]], vCodec: Codec[V, Array[Byte]]) = {
    new BinaryVersionedSource(path, sourceVersion, sinkVersion) with KeyValueCodecTransformer[K,Array[Byte],V,Array[Byte]] {
      val safeKeyCodec = new MeatLocker(kCodec)
      val safeValCodec = new MeatLocker(vCodec)
      override def keyCodec = safeKeyCodec.get
      override def valCodec = safeValCodec.get
    }
  }

  def increment(reducers: Int = 1)(implicit monoid: Monoid[V], flowDef: FlowDef, mode: Mode): KeyValThing[K,V]
  def pack[K1,K2](implicit codec: Codec[K,(K1,K2)]): KeyValThing[K1,Map[K2,List[V]]]
}

object Thing {
  def decoded[K,V](implicit kCodec: Codec[K, Array[Byte]], vCodec: Codec[V, Array[Byte]]) =
    self andThen new KeyValueCodecTransformer[K,Array[Byte],V,Array[Byte]] {
      val safeKeyCodec = new MeatLocker(kCodec)
      val safeValCodec = new MeatLocker(vCodec)
      override def keyCodec = safeKeyCodec.get
      override def valCodec = safeValCodec.get
    }
}


/*
  def incremental[K,V](reducerArgs: Int = 1)(implicit m: Monoid[V], flowDef: FlowDef, mode: Mode) =
    self andThen new IncrementalTransformer[K, V] {
      implicit val monoid: Monoid[V] = m
      def baseSrc: Option[Pipe] = if (resourceExists(mode)) Some(read) else None
      val reducers: Int = reducerArgs
    }
*/

/*
  def pack[K1, K2](implicit codec: Codec[K, (K1, K2)],
    kCodec: Codec[K1, Array[Byte]],
    vCodec: Codec[Map[K2, List[V]], Array[Byte]]) =
    self
      .andThen(new PackTransformer[K, K1, K2, V] { override def innerCodec = codec })
      .andThen(new KeyValueCodecTransformer[K1,Array[Byte],Map[K2,List[V]],Array[Byte]] {

      })

*/


// TODO: Move the following traits into Scalding.

object PipeTransformer {
  implicit val monoid: Monoid[PipeTransformer] = PipeTransformerMonoid
}

object PipeTransformerMonoid extends Monoid[PipeTransformer] {
  override lazy val zero = new PipeTransformer {}
  override def plus(a: PipeTransformer, b: PipeTransformer) = a andThen b
}

trait PipeTransformer { self =>
  def onRead(pipe: Pipe): Pipe
  def onWrite(pipe: Pipe): Pipe

  /**
   * Composes two instances of PipeTransformer into a new PipeTransformer
   * with this one applied first on write and last on read.
   *
   * onWrite: pipe --> (this transformer) --> (that transformer)
   * onRead: (this transformer) <-- (that transformer) <-- pipe
   */
  def andThen(that: PipeTransformer) = {
    new PipeTransformer {
      def onRead(pipe: Pipe) = self.onRead(that.onRead(pipe))
      def onWrite(pipe: Pipe) = that.onWrite(self.onWrite(pipe))
    }
  }

  /**
   * Composes two instances of PipeTransformer into a new PipeTransformer
   * with this one applied last on write and first on read.
   *
   * onWrite: pipe --> (that transformer) --> (this transformer)
   * onRead: (that transformer) <-- (this transformer) <-- pipe
   */
  def compose(that: PipeTransformer) = {
    new PipeTransformer {
      def onRead(pipe: Pipe) = that.onRead(self.onRead(pipe))
      def onWrite(pipe: Pipe) = self.onWrite(that.onWrite(pipe))
    }
  }
}

trait IncrementalTransformer[K, V] extends PipeTransformer { self =>
  import Dsl._

  private def appendToken(pipe: Pipe, triplet: Fields, token: Int) =
    pipe.mapTo((0, 1) -> triplet) { pair: (K, V) => pair :+ token }

  implicit def monoid: Monoid[V]
  def baseSrc: Option[Pipe]
  def reducers: Int = 1

  abstract override def onRead(pipe: Pipe) = super.onRead(pipe)
  abstract override def onWrite(pipe: Pipe) =
    super.onWrite(baseSrc map { src =>
      val fnames = ('key, 'value, 'isNew)
      val oldPairs = appendToken(src, fnames, 0)
      val newPairs = appendToken(pipe, fnames, 1)
      (oldPairs ++ newPairs)
        .groupBy('key) { _.reducers(reducers).sortBy('isNew).plus[V]('value) }
        .project(('key, 'value))
    } getOrElse pipe)
}

trait PackTransformer[K, K1, K2, V] extends PipeTransformer {
  import Dsl._

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

  abstract override def onWrite(pipe: Pipe) =
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

  abstract override def onRead(pipe: Pipe) =
    super.onRead(pipe).mapTo((0, 1) -> (0, 1)) { pair: (K1, V1) =>
      (keyCodec.decode(pair._1), valCodec.decode(pair._2))
    }

  abstract override def onWrite(pipe: Pipe) =
    super.onWrite(pipe
      .mapTo((0, 1) -> ('key, 'value)) { pair: (K, V) =>
        (keyCodec.encode(pair._1), valCodec.encode(pair._2))
      })
}

trait RenameTransformer extends PipeTransformer {
  import Dsl._

  def keyField: String
  def valField: String

  abstract override def onRead(pipe: Pipe) = super.onRead(pipe)
  abstract override def onWrite(pipe: Pipe) = super.onWrite(pipe.rename((0, 1) -> ('key, 'value)))
}
