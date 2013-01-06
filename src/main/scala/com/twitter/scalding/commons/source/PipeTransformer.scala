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
import com.twitter.algebird.Monoid

// TODO: Move the following traits into Scalding.

object PipeTransformer {
  implicit val monoid: Monoid[PipeTransformer] = PipeTransformerMonoid
}

object PipeTransformerMonoid extends Monoid[PipeTransformer] {
  override lazy val zero = new PipeTransformer {}
  override def plus(a: PipeTransformer, b: PipeTransformer) = a andThen b
}

trait PipeTransformer { self =>
  def onRead(pipe: Pipe): Pipe = pipe
  def onWrite(pipe: Pipe): Pipe = pipe

  /**
   * Composes two instances of PipeTransformer into a new PipeTransformer
   * with this one applied first on write and last on read.
   *
   * onWrite: pipe --> (this transformer) --> (that transformer)
   * onRead: (this transformer) <-- (that transformer) <-- pipe
   */
  def andThen(that: PipeTransformer) = {
    new PipeTransformer {
      override def onRead(pipe: Pipe) = self.onRead(that.onRead(pipe))
      override def onWrite(pipe: Pipe) = that.onWrite(self.onWrite(pipe))
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
      override def onRead(pipe: Pipe) = that.onRead(self.onRead(pipe))
      override def onWrite(pipe: Pipe) = self.onWrite(that.onWrite(pipe))
    }
  }
}
