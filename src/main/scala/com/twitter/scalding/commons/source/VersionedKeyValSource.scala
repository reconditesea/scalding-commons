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
import com.twitter.bijection.{ Bijection, Pivot }
import com.twitter.chill.MeatLocker
import com.twitter.scalding._
import org.apache.hadoop.mapred.JobConf

/**
 * Source used to write key-value pairs as byte arrays into a versioned store.
 * Supports incremental updates via the monoid on V.
 */

trait KeyValProgression[K,V] extends PipeTransformer {
  def toSource(implicit bij: Bijection[(K,V), (Array[Byte],Array[Byte])]): BinaryVersionedSource
  def incremental(reducers: Int)(implicit monoid: Monoid[V], flowDef: FlowDef, mode: Mode): VersionedKeyValSource[K,V]
  def pack[K1,K2](reducers: Int)(implicit pivot: Pivot[(K,V),K1,(K2,V)]): VersionedKeyValSource[K1,Map[K2,Iterable[V]]]
}

object VersionedKeyValSource {
  val DEFAULT_KEY_FIELD = "key"
  val DEFAULT_VALUE_FIELD = "value"
  /**
  * Returns a versioned key-value source that transforms kv-pairs to
  * byte-only sequencefiles and back.
  */
  def apply[K,V](path: String,
                 sourceVersion: Option[Long] = None,
                 sinkVersion: Option[Long] = None,
                 keyField: String = DEFAULT_KEY_FIELD,
                 valField: String = DEFAULT_VALUE_FIELD)
  (implicit bijection: Bijection[(K,V), (Array[Byte],Array[Byte])]) =
    new VersionedKeyValSource[K,V](path, sourceVersion, sinkVersion, keyField, valField).toSource

  /**
   * Returns a VersionedKeyValSource that performs an incremental
   * update on the previous version (or sourceVersion) before
   * converting to bytes.
   */
  def incremental[K,V](path: String,
                       sourceVersion: Option[Long] = None,
                       sinkVersion: Option[Long] = None,
                       keyField: String = DEFAULT_KEY_FIELD,
                       valField: String = DEFAULT_VALUE_FIELD,
                       reducers: Int = 1)
  (implicit monoid: Monoid[V], bijection: Bijection[(K,V), (Array[Byte],Array[Byte])], flowDef: FlowDef, mode: Mode) =
    new VersionedKeyValSource[K,V](path, sourceVersion, sinkVersion, keyField, valField)
      .incremental(reducers)
      .toSource

  /** Returns a VersionedKeyValSource that pivots kv-pairs out of the
   * original K,V type into pairs of K1,Map[K2,List[V]]. Reading
   * reconstructs the original kv pairs.
   */
  def packed[K,K1,K2,V](path: String,
                        sourceVersion: Option[Long] = None,
                        sinkVersion: Option[Long] = None,
                        keyField: String = DEFAULT_KEY_FIELD,
                        valField: String = DEFAULT_VALUE_FIELD,
                        reducers: Int = 1)
  (implicit bijection: Bijection[K,(K1,K2)], serBij: Bijection[(K1,Map[K2,List[V]]), (Array[Byte],Array[Byte])]) =
     new VersionedKeyValSource[K,V](path, sourceVersion, sinkVersion, keyField, valField)
      .pack(reducers)
      .toSource

  // Returns a VersionedKeyValSource that performs an incremental
  // merge before applying packing operations and serialization (on
  // write).
  def packedIncremental[K,K1,K2,V](path: String,
                                   sourceVersion: Option[Long] = None,
                                   sinkVersion: Option[Long] = None,
                                   incrementReducers: Int = 1,
                                   packReducers: Int = 1,
                                   keyField: String = DEFAULT_KEY_FIELD,
                                   valField: String = DEFAULT_VALUE_FIELD)
  (implicit monoid: Monoid[V],
   bijection: Bijection[K,(K1,K2)],
   serBij: Bijection[(K1,Map[K2,List[V]]),(Array[Byte],Array[Byte])],
   flowDef: FlowDef,
   mode: Mode) =
     new VersionedKeyValSource[K,V](path, sourceVersion, sinkVersion, keyField, valField)
      .incremental(incrementReducers)
      .pack(packReducers)
      .toSource
}

class VersionedKeyValSource[K,V](path: String,
                                 sourceVersion: Option[Long],
                                 sinkVersion: Option[Long],
                                 override val keyField: String,
                                 override val valField: String)
extends VersionedSource[(K,V)](path, sourceVersion, sinkVersion)
with RenameTransformer { self =>
  import Dsl._

  override def hdfsScheme =
    HadoopSchemeInstance(new KeyValueByteScheme(new Fields(keyField, valField)))

  def toSource(implicit bij: Bijection[(K,V),(Array[Byte],Array[Byte])]) =
    new VersionedKeyValSource[K,V](path, sourceVersion, sinkVersion, keyField, valField) with KeyValueBijectionTransformer[K,V,Array[Byte],Array[Byte]] {
      override val bijection = implicitly[Bijection[(K,V),(Array[Byte],Array[Byte])]]
      override def onRead(pipe: Pipe) = this.onRead(self.onRead(pipe))
      override def onWrite(pipe: Pipe) = self.onWrite(this.onWrite(pipe))
    }

  def incremental(numReducers: Int)
  (implicit mv: Monoid[V], flowDef: FlowDef, mode: Mode): VersionedKeyValSource[K,V] = {
    new VersionedKeyValSource[K,V](path, sourceVersion, sinkVersion, keyField, valField) with IncrementalTransformer[K,V] {
      override implicit val monoid = mv
      // TODO: KeyVal should accept a supplier function that returns
      // an Option[Pipe]. By default, return None -- at the last
      // minute, in the final step, replace this business with the
      // proper resourceExists shit.
      override def baseSrc = if (resourceExists(mode)) Some(read) else None
      override def incrementalReducers = numReducers
      override def onRead(pipe: Pipe) = this.onRead(self.onRead(pipe))
      override def onWrite(pipe: Pipe) = self.onWrite(this.onWrite(pipe))
    }
  }
  def pack[K1,K2](reducers: Int)(implicit bijection: Bijection[K,(K1,K2)], serBij: Bijection[(K1,Map[K2,List[V]]), (Array[Byte],Array[Byte])]) =
    new VersionedKeyValSource[K1,Map[K2,List[V]]](path, sourceVersion, sinkVersion, keyField, valField) with PackTransformer[K,K1,K2,V] {
      override val lens = bijection
      override val packReducers = reducers
      override def onRead(pipe: Pipe) = this.onRead(self.onRead(pipe))
      override def onWrite(pipe: Pipe) = self.onWrite(this.onWrite(pipe))
    }
}
