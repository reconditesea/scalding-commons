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
import cascading.tuple.Fields
import com.twitter.scalding.Dsl
import com.twitter.scalding.HadoopSchemeInstance
import com.twitter.scalding.Mappable
import com.twitter.scalding.TupleConverter

/**
 * Versioned source dealing with bare bytes.
 */

object BinaryVersionedSource {
  def apply(path: String,
            sourceVersion: Option[Long] = None,
            sinkVersion: Option[Long] = None,
            keyField: String = "key",
            valField: String = "value") =
    new BinaryVersionedSource(path, sourceVersion, sinkVersion, keyField, valField)

}
class BinaryVersionedSource(path: String,
                            sourceVersion: Option[Long],
                            sinkVersion: Option[Long],
                            override val keyField: String = "key",
                            override val valField: String = "value")
extends VersionedSource[(Array[Byte],Array[Byte])](path, sourceVersion, sinkVersion)
with RenameTransformer {
  override def hdfsScheme =
    HadoopSchemeInstance(new KeyValueByteScheme(new Fields(keyField, valField)))
}
