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
import com.twitter.algebird.Monoid
import com.twitter.bijection.Bijection
import com.twitter.scalding._

// TODO: Move the following traits into Scalding.

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
  def lens: Bijection[K,(K1,K2)]

  override def onRead(pipe: Pipe) =
    super.onRead(pipe)
      .flatMap((0, 1) -> ('key, 'value)) { pair: (K1, Map[K2, List[V]]) =>
        val (k1, m) = pair
        m flatMap { case (k2, lv) =>
          val k = lens.invert((k1, k2))
          lv map { (k, _) }
        }
      }

  override def onWrite(pipe: Pipe) =
    super.onWrite(pipe
      .mapTo((0, 1) -> ('key, 'value)) { pair: (K, V) =>
        val (k, v) = pair
        val (k1, k2) = lens(k)
        (k1, Map(k2 -> v))
      }
      .groupBy('key) { _.plus[Map[K2, List[V]]]('value) })
}

trait KeyValueBijectionTransformer[K, V, K1, V1] extends PipeTransformer {
  import Dsl._

  def bijection: Bijection[(K,V), (K1,V1)]

  override def onRead(pipe: Pipe) =
    super.onRead(pipe).mapTo((0, 1) -> (0, 1)) { pair: (K1, V1) =>
      bijection.invert(pair)
    }

  override def onWrite(pipe: Pipe) =
    super.onWrite(pipe
      .mapTo((0, 1) -> ('key, 'value)) { pair: (K, V) =>
        bijection(pair)
      })
}

trait RenameTransformer extends PipeTransformer {
  import Dsl._

  def keyField: String
  def valField: String

  override def onRead(pipe: Pipe) = super.onRead(pipe)
  override def onWrite(pipe: Pipe) = super.onWrite(pipe.rename((0, 1) -> (keyField, valField)))
}
