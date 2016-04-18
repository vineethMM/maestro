//   Copyright 2015 Commonwealth Bank of Australia
//
//   Licensed under the Apache License, Version 2.0 (the "License");
//   you may not use this file except in compliance with the License.
//   You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
//   Unless required by applicable law or agreed to in writing, software
//   distributed under the License is distributed on an "AS IS" BASIS,
//   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
//   See the License for the specific language governing permissions and
//   limitations under the License.

package au.com.cba.omnia.maestro.scalding

// TODO: trim these imports
import scala.concurrent.{ Await, Future, ExecutionContext => ConcurrentExecutionContext, Promise }

import scala.util.{ Failure, Success, Try }

import scalaz._, Scalaz._

import cascading.flow.{ FlowDef, Flow }

import com.twitter.scalding.{Execution, Config, Mode, Args, ExecutionCounters}
import com.twitter.scalding.{UniqueID, JobStats, TypedPipe, TypedSink}

import com.twitter.algebird.{ Monoid, Monad => AlgMonad, Semigroup }

import au.com.cba.omnia.omnitool.{Result, RelMonad, ResultantOps, ToResultantMonadOps}

import au.com.cba.omnia.omnitool.%~>._

/** Extend this and import to use operations via ExecutionR.zip, etc., and enrich M with RichExecutionR. */
trait ExecutionROps[M[_]] extends ToResultantMonadOps {

  // Relative monad instances
  implicit def ExecRel:   Execution %~> M
  implicit def ResultRel: Result    %~> M

  private def monad: Monad[M] = ResultRel

  object ExecutionR {

    /** An algebird Monad instance for M */  // TODO: Check if we need this.
    implicit object ExecutionRMonad extends com.twitter.algebird.Monad[M] {
      override def apply[T](t: T): M[T] = from(t)
      override def map[T, U](e: M[T])(fn: T => U): M[U] = monad.map(e)(fn)
      override def flatMap[T, U](e: M[T])(fn: T => M[U]): M[U] = monad.bind(e)(fn)
      override def join[T, U](t: M[T], u: M[U]): M[(T, U)] = t.zip(u)
    }

    // Do we need/want these here?  Maybe yes, but there could be conflicts with implicits mixed from elsewhere.
    // So, maybe they should be non-implicit.
    def semigroup[T: Semigroup]: Semigroup[M[T]] = Semigroup.from[M[T]] { (a, b) =>
      a.zip(b).map { case (ta, tb) => Semigroup.plus(ta, tb) }
    }
    def monoid[T: Monoid]: Monoid[M[T]] = Monoid.from(ExecutionR.from(Monoid.zero[T])) { (a, b) =>
      a.zip(b).map { case (ta, tb) => Monoid.plus(ta, tb) }
    }

    // Some of these may be redundant given ResultantMonad
    def failed(t: Throwable): M[Nothing] = ExecRel.rPoint(Execution.failed(t))
    def from[T](t: => T): M[T]           = ExecRel.rPoint(Execution.from(t))
    //def fromTry[T](t: => Try[T]): M[T]   = ExecRel.rPoint(Execution.fromTry(t))  // Added after scalding 13.1

    def fromFuture[T](fn: ConcurrentExecutionContext => Future[T]): M[T]
      = ExecRel.rPoint(Execution.fromFuture(fn))

    val unit: M[Unit]                                  = ExecRel.rPoint(Execution.from(()))
    def fromFn(fn: (Config, Mode) => FlowDef): M[Unit] = ExecRel.rPoint(Execution.fromFn(fn))

    //def getArgs: M[Args]                 = ExecRel.rPoint(Execution.getArgs())  // Added after scalding 13.1

    def getConfig: M[Config]             = ExecRel.rPoint(Execution.getConfig)
    def getMode: M[Mode]                 = ExecRel.rPoint(Execution.getMode)
    def getConfigMode: M[(Config, Mode)] = ExecRel.rPoint(Execution.getConfigMode)

    def withArgs[T](fn: Args => M[T]): M[T] =
      getConfig.flatMap { conf => fn(conf.getArgs) }

    def withId[T](fn: UniqueID => M[T]): M[T] = monad.join(ExecRel.rPoint(
      Execution.withId(id => Execution.from(fn(id)))       // via Execution[M[T]] then M[M[T]]
    ))

    def zip[A, B](ax: M[A], bx: M[B]): M[(A, B)] =
      ax.zip(bx)
    def zip[A, B, C](ax: M[A], bx: M[B], cx: M[C]): M[(A, B, C)] =
      ax.zip(bx).zip(cx).map { case ((a, b), c) => (a, b, c) }

    def zip[A, B, C, D](ax: M[A], bx: M[B], cx: M[C], dx: M[D]): M[(A, B, C, D)] =
      ax.zip(bx).zip(cx).zip(dx).map { case (((a, b), c), d) => (a, b, c, d) }

    def zip[A, B, C, D, E](ax: M[A], bx: M[B], cx: M[C], dx: M[D], ex: M[E]): M[(A, B, C, D, E)] =
      ax.zip(bx).zip(cx).zip(dx).zip(ex).map { case ((((a, b), c), d), e) => (a, b, c, d, e) }

    // This could also be implemented via Execution.sequence.
    def sequence[T](exs: Seq[M[T]]): M[Seq[T]] = {
      @annotation.tailrec
      def go(xs: List[M[T]], acc: M[List[T]]): M[List[T]] = xs match {
        case Nil => acc
        case h :: tail => go(tail, h.zip(acc).map { case (y, ys) => y :: ys })
      }
      // This pushes all of them onto a list, and then reverse to keep order
      go(exs.toList, from(Nil)).map(_.reverse)
    }
  }

  implicit class RichExecutionR[T](val self: M[T]) {

    def getCounters: M[(T, ExecutionCounters)]         = ExecRel.rMap(self)(_.getCounters)
    def getAndResetCounters: M[(T, ExecutionCounters)] = ExecRel.rMap(self)(_.getAndResetCounters)
    def onComplete(fn: Try[T] => Unit): M[T]           = ExecRel.rMap(self)(_.onComplete(fn))
    def resetCounters: M[T]                            = ExecRel.rMap(self)(_.resetCounters)

    // M should generally define run returning Execution[T], which is run eventually via Execution.run

    def unit: M[Unit] = self.map(_ => ())

    /** run the executions from this and that in parallel, without any dependency, in a single cascading
      * flow if possible.  Effects from M on top of Execution are sequenced via rBind.
      */
    def zip[U](that: M[U]): M[(T, U)] = ExecRel.rBind(self)((thisExec: Execution[T]) =>
      ExecRel.rMap(that)((thatExec: Execution[U]) => thisExec.zip(thatExec))
    )
  }

  implicit class RichExecutionCrossMaps[T](val self: Execution[T]) {
    def flatMap[S](f: T => M[S]): M[S] = ExecRel.rPoint(self).flatMap(f)
    def map[S](f: T => S): M[S] = ExecRel.rPoint(self).map(f)
  }

  implicit class RichTypedPipeR[T](val self: TypedPipe[T]) {
    def writeExecutionR(dest: TypedSink[T]): M[Unit] = ExecRel.rPoint(self.writeExecution(dest))
  }

  implicit def fromEx[T](ex: Execution[T]): M[T] = ExecRel.rPoint(ex)
}
