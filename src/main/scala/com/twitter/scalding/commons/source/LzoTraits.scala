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
import cascading.scheme.local.{ TextDelimited => CLTextDelimited, TextLine => CLTextLine }
import cascading.tuple.Fields
import com.google.protobuf.Message
import com.twitter.elephantbird.cascading2.scheme._
import com.twitter.scalding._
import com.twitter.scalding.Dsl._
import org.apache.hadoop.mapred.{ JobConf, OutputCollector, RecordReader }
import org.apache.thrift.TBase

// Scala is pickier than Java about type parameters, and Cascading's Scheme
// declaration leaves some type parameters underspecified.  Fill in the type
// parameters with wildcards so the Scala compiler doesn't complain.

object HadoopSchemeInstance {
  def apply(scheme: Scheme[_, _, _, _, _]) =
    scheme.asInstanceOf[Scheme[JobConf, RecordReader[_, _], OutputCollector[_, _], _, _]]
}

trait LzoProtobuf[T <: Message] extends Mappable[T] {
  def column: Class[_]
  override def localScheme = { println("This does not work yet"); new CLTextLine }
  override def hdfsScheme = HadoopSchemeInstance(new LzoProtobufScheme[T](column))
  override val converter = Dsl.singleConverter[T]
}

trait LzoThrift[T <: TBase[_, _]] extends Mappable[T] {
  def column: Class[_]
  override def localScheme = { println("This does not work yet"); new CLTextLine }
  override def hdfsScheme = HadoopSchemeInstance(new LzoThriftScheme[T](column))
  override val converter = Dsl.singleConverter[T]
}

trait LzoText extends Mappable[String] {
  def column = classOf[String]
  override def localScheme = { println("This does not work yet"); new CLTextLine }
  override def hdfsScheme = HadoopSchemeInstance(new LzoTextLine())
  override val converter = Dsl.singleConverter[String]
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
