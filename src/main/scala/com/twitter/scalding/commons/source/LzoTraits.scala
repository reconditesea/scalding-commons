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

import collection.mutable.ListBuffer

import cascading.pipe.Pipe
import cascading.scheme.local.{ TextDelimited => CLTextDelimited }

import com.twitter.bijection.Bijection
import com.twitter.bijection.thrift.BinaryThriftCodec
import com.twitter.bijection.protobuf.ProtobufCodec
import com.twitter.chill.MeatLocker
import com.twitter.elephantbird.cascading2.scheme.{ LzoByteArrayScheme, LzoTextDelimited }
import com.twitter.scalding._
import com.twitter.scalding.Dsl._

trait LzoCodec[T] extends FileSource with Mappable[T] {
  def bijection: Bijection[T,Array[Byte]]
  override def localPath = sys.error("Local mode not yet supported.")
  override def hdfsScheme = HadoopSchemeInstance(new LzoByteArrayScheme)
  override val converter = Dsl.singleConverter[T]
  override def transformForRead(pipe: Pipe) =
    pipe.map(0 -> 0) { bijection.invert(_: Array[Byte]) }

  override def transformForWrite(pipe: Pipe) =
    pipe.mapTo(0 -> 0) { bijection.apply(_: T) }
}

trait ErrorHandlingLzoCodec[T] extends LzoCodec[T] {
  def check(errors: Iterable[Throwable])

  val errors = ListBuffer[Throwable]()

  override def transformForRead(pipe: Pipe) =
    pipe.flatMap(0 -> 0) { bytes: Array[Byte] =>
      try {
        Some(bijection.invert(bytes))
      } catch {
        case e =>
          // TODO: use proper logging
          e.printStackTrace()
          errors += e
          check(errors)
          None
      }
    }
}

trait ErrorThresholdLzoCodec[T] extends ErrorHandlingLzoCodec[T] {
  def maxErrors: Int
  override def check(errors: Iterable[Throwable]) {
    if (errors.size > maxErrors) {
      throw new RuntimeException("Error count exceeded the threshold of " + maxErrors)
    }
  }
}

trait LzoTsv extends DelimitedScheme {
  override def localScheme = { println("This does not work yet"); new CLTextDelimited(fields, separator, types) }
  override def hdfsScheme = HadoopSchemeInstance(new LzoTextDelimited(fields, separator, types))
}

trait LzoTypedTsv[T] extends DelimitedScheme with Mappable[T] {
  override def localScheme = { println("This does not work yet"); new CLTextDelimited(fields, separator, types) }
  override def hdfsScheme = HadoopSchemeInstance(new LzoTextDelimited(fields, separator, types))

  val mf: Manifest[T]

  override val types: Array[Class[_]] = {
    if (classOf[scala.Product].isAssignableFrom(mf.erasure)) {
      //Assume this is a Tuple:
      mf.typeArguments.map { _.erasure }.toArray
    } else {
      //Assume there is only a single item
      Array(mf.erasure)
    }
  }

  protected def getTypeHack(implicit m: Manifest[T], c: TupleConverter[T]) = (m, c)
}
