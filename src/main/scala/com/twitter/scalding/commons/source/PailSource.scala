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

import backtype.cascading.tap.PailTap
import backtype.hadoop.pail.{Pail, PailStructure}
import cascading.pipe.Pipe
import cascading.scheme.Scheme
import cascading.tap.Tap
import com.twitter.bijection.Injection
import com.twitter.chill.MeatLocker
import com.twitter.scalding._
import java.util.{ List => JList }
import org.apache.hadoop.mapred.{ JobConf, OutputCollector, RecordReader }
import scala.collection.JavaConverters._

/**
 * The PailSource enables scalding integration with the Pail class in the
 * dfs-datastores library. PailSource allows scalding to sink 1-tuples
 * to subdirectories of a root folder by applying a routing function to
 * each tuple.
 */

// targetFn takes an instance of T and returns a list
// of "path components". Pail joins these components with
// File.separator and sinks the instance of T into the pail at that location.
//
// Usual implementations of "validator" will check that the length of
// the supplied list is >= the length f the list returned by targetFn.
//
// CodecPailStructure has a default constructor because it is instantiated via reflection
// This unfortunately means params must be set via setParams to make it useful
// See the PailSource.apply(..) method for an example of setParams
// Another detailed example below:

/*
Example Usage: Lets store 100 numbers in a pail!
Numbers below 50 land in the first tree, above 50 in the second
Furthermore, we ceate 7 subtrees under each tree, based on (number mod 7)
This gives us a nice deep directory structure created on the fly

class PailTest(args : Args) extends Job(args) {
  val pipe = IterableSource((1 to 100), "src").read
  val func = ((obj:Int) => if( obj < 50) List("./belowfifty" + (obj % 7)) else List("./abovefifty" + (obj % 7)))
  val validator = ((x:List[String])=>true)
  val mytype = classOf[Int]
  val injection = new NumericInjections{}.int2BigEndian
  val sink = PailSource[Int]( "pailtest", func, validator, mytype, injection)
  pipe.write(sink)
}
*/

class CodecPailStructure[T] extends PailStructure[T] {

  var targetFn: T => List[String] = null
  var validator :List[String] => Boolean = null
  var mytype: java.lang.Class[T] = null
  var injection: Injection[T, Array[Byte]] = null

  def setParams(  targetFn: T => List[String],
                  validator: List[String] => Boolean,
                  mytype:java.lang.Class[T],
                  injection: Injection[T, Array[Byte]]) = {

    this.targetFn = targetFn
    this.validator = validator
    this.mytype = mytype
    this.injection = injection
  }
  override def isValidTarget(paths: String*): Boolean = validator(paths.toList)
  override def getTarget(obj: T): JList[String] = targetFn(obj).toList.asJava
  override def serialize(obj: T): Array[Byte] = injection.apply(obj)
  override def deserialize(bytes: Array[Byte]): T = injection.invert(bytes).get
  override val getType = mytype
}

object PailSource {
  // Generic version of PailSource accepts a PailStructure.
  def sink[T](rootPath: String, structure: PailStructure[T]) =
    new PailSource(rootPath, structure)

  // A PailSource can also build its structure on the fly from a
  // couple of functions.
  def sink[T]( rootPath: String,
                targetFn: (T) => List[String],
                validator: (List[String]) => Boolean,
                mytype:java.lang.Class[T],
                injection: Injection[T, Array[Byte]]) = {

    val cps = new CodecPailStructure[T]()
    cps.setParams( targetFn, validator, mytype, injection)
    new PailSource(rootPath, cps)
  }

  def source[T](rootPath: String, structure: PailStructure[T], subPaths: Array[List[String]]) = {
    assert( subPaths != null && subPaths.size > 0)
    new PailSource(rootPath, structure, subPaths)
  }
}

class PailSource[T] private (rootPath: String, structure: PailStructure[T], subPaths: Array[List[String]] = null)
extends Source with Mappable[T] {
  import Dsl._

  override val converter = singleConverter[T]
  val fieldName = "pailItem"

  lazy val getTap = {
    val spec = PailTap.makeSpec(null, structure)
    val javaSubPath = if ((subPaths == null) || (subPaths.size == 0)) null else subPaths map { _.asJava }
    val opts = new PailTap.PailTapOptions(spec, fieldName, javaSubPath , null)
    new PailTap(rootPath, opts)
  }

  override def hdfsScheme = getTap.getScheme
    .asInstanceOf[Scheme[JobConf, RecordReader[_, _], OutputCollector[_, _], Array[Object], Array[Object]]]

  override def createTap(readOrWrite: AccessMode)(implicit mode: Mode): Tap[_, _, _] = {
    val tap = castHfsTap(getTap)

    mode match {
      case Hdfs(strict, config) =>
        readOrWrite match {
          case Read  => tap
          case Write => tap
        }
      case _ => super.createTap(readOrWrite)(mode)
    }
  }

  override def transformForWrite(pipe: Pipe) = pipe
}
