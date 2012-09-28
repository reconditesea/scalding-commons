package com.twitter.scalding.source

import backtype.cascading.scheme.KeyValueByteScheme
import backtype.cascading.tap.VersionedTap
import backtype.cascading.tap.VersionedTap.TapMode

import cascading.flow.FlowDef
import cascading.pipe.Pipe
import cascading.scheme.Scheme
import cascading.tap.Tap
import cascading.tuple.Fields

import com.twitter.algebird.Monoid
import com.twitter.util.Codec
import com.twitter.scalding._
import com.twitter.scalding.serialization.MeatLocker

import org.apache.hadoop.mapred.JobConf
import org.apache.hadoop.mapred.OutputCollector
import org.apache.hadoop.mapred.RecordReader

/**
 * Source used to write key-value pairs as byte arrays into a versioned store.
 * Supports incremental updates via the monoid on V.
 */

case class VersionedKeyValSource[K,V](path: String, version: Option[Long] = None)
(@transient implicit val keyCodec: Codec[K,Array[Byte]],
 @transient valCodec: Codec[V,Array[Byte]]) extends Source {
  import Dsl._

  val keyField = "key"
  val valField = "value"
  val safeKeyCodec = new MeatLocker(keyCodec)
  val safeValCodec = new MeatLocker(valCodec)

  override def hdfsScheme =
    new KeyValueByteScheme(new Fields(keyField, valField))
      .asInstanceOf[Scheme[JobConf, RecordReader[_,_], OutputCollector[_,_], Array[Object], Array[Object]]]

  def getTap(mode: TapMode) = {
    val tap = new VersionedTap(path, keyField, valField, mode)
    if (version.isDefined)
      tap.setVersion(version.get)
    else
      tap
  }

  val source = getTap(TapMode.SOURCE)
  val sink = getTap(TapMode.SINK)

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

  override def transformForRead(pipe: Pipe) = {
    pipe.map(('key,'value) -> ('key,'value)) { pair: (Array[Byte],Array[Byte]) =>
      (safeKeyCodec.get.decode(pair._1), safeValCodec.get.decode(pair._2))
    }
  }

  override def transformForWrite(pipe: Pipe) = {
    pipe.mapTo((0,1) -> ('key,'value)) { pair: (K,V) =>
      (safeKeyCodec.get.encode(pair._1), safeValCodec.get.encode(pair._2))
    }
  }
}

object RichPipeEx extends FieldConversions with TupleConversions with java.io.Serializable {
  implicit def pipeToRichPipeEx(pipe: Pipe): RichPipeEx = new RichPipeEx(pipe)
  implicit def typedPipeToRichPipeEx[K: Ordering, V: Monoid](pipe: TypedPipe[(K,V)]) =
    new TypedRichPipeEx(pipe)
}

class TypedRichPipeEx[K: Ordering, V: Monoid](pipe: TypedPipe[(K,V)]) extends java.io.Serializable {
  import Dsl._
  import TDsl._

  // VersionedKeyValSource always merges with the most recent complete
  // version
  def writeIncremental(path: String)
  (implicit flowDef: FlowDef, mode: Mode,
   keyCodec: Codec[K,Array[Byte]],
   valCodec: Codec[V,Array[Byte]]) = {

    val src = VersionedKeyValSource[K,V](path)
    val conf = mode.asInstanceOf[HadoopMode].jobConf.asInstanceOf[JobConf]
    val outPipe =
      if (!src.source.resourceExists(conf))
        pipe
      else {
        val oldPairs = TypedPipe
          .from[(K,V)](src.read, (0,1))
          .map { _ :+ 0 }

        val newPairs = pipe.map { _ :+ 1 }

        (oldPairs ++ newPairs)
          .groupBy {  _._1 }
          .sortBy { _._3 }
          .mapValues { _._2 }
          .sum
      }

    outPipe.write((0,1), VersionedKeyValSource[K,V](path))
  }
}

class RichPipeEx(pipe: Pipe) extends java.io.Serializable {
  import Dsl._

  // VersionedKeyValSource always merges with the most recent complete
  // version
  def writeIncremental[K,V](path: String)
  (implicit monoid: Monoid[V],
   flowDef: FlowDef,
   mode: Mode,
   keyCodec: Codec[K,Array[Byte]],
   valCodec: Codec[V,Array[Byte]]) = {

    def appendToken(pipe: Pipe, token: Int) =
      pipe.mapTo((0,1) -> ('key,'value,'isNew)) { pair: (K,V) => pair :+ token }

    val src = VersionedKeyValSource[K,V](path)
    val conf = new JobConf(mode.asInstanceOf[HadoopMode].jobConf)
    val outPipe =
      if (!src.source.resourceExists(conf))
        pipe
      else {
        val oldPairs = appendToken(src.read, 0)
        val newPairs = appendToken(pipe, 1)

        (oldPairs ++ newPairs)
          .groupBy('key) { _.sortBy('isNew).plus[V]('value) }
          .project(('key,'value))
      }

    outPipe.write(VersionedKeyValSource[K,V](path))
  }
}
