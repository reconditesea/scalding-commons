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

import com.twitter.bijection.Bijection
import com.twitter.bijection.protobuf.ProtobufCodec
import com.twitter.bijection.thrift.BinaryThriftCodec
import com.twitter.chill.MeatLocker
import com.google.protobuf.Message
import com.twitter.scalding._
import com.twitter.scalding.Dsl._
import org.apache.thrift.TBase

abstract class FixedPathLzoCodec[T](paths: String*)
(implicit @transient suppliedBijection: Bijection[T, Array[Byte]])
  extends FixedPathSource(paths: _*) with LzoCodec[T] {
  val boxed = MeatLocker(suppliedBijection)
  override def bijection = boxed.get
}

@deprecated("Use FixedPathLzoCodec[T] with bijection.thrift.BinaryThriftCodec[T]", "0.1.2")
abstract class FixedPathLzoThrift[T <: TBase[_, _]: Manifest](paths: String*)
  extends FixedPathLzoCodec[T](paths: _*)(BinaryThriftCodec[T])

@deprecated("Use FixedPathLzoCodec[T] with bijection.protobuf.ProtobufCodec[T]", "0.1.2")
abstract class FixedPathLzoProtobuf[T <: Message: Manifest](paths: String*)
  extends FixedPathLzoCodec[T](paths: _*)(ProtobufCodec[T])
