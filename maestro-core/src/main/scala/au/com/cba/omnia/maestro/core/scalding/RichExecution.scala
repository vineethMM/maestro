//   Copyright 2014 Commonwealth Bank of Australia
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

package au.com.cba.omnia.maestro.core.scalding

import scala.concurrent.Future

import scalaz._, Scalaz._
import scalaz.\&/.{This, That, Both}

import com.twitter.scalding.{Config, Execution}

import org.apache.hadoop.hive.conf.HiveConf

import au.com.cba.omnia.omnitool.Result

import au.com.cba.omnia.permafrost.hdfs.Hdfs

import au.com.cba.omnia.ebenezer.scrooge.hive.Hive

/** Pimps an Execution instance. */
case class RichExecution[A](execution: Execution[A]) {
  import ExecutionOps._

  def withSubConfig(modifyConfig: Config => Config): Execution[A] =
    Execution.getConfigMode.flatMap { case (config, mode) =>
      Execution.fromFuture(cec => execution.run(modifyConfig(config), mode)(cec))
    }

  /** Like "finally", but only performs the final action if there was an error. */
  def onException[B](action: Execution[B]): Execution[A] =
    execution.recoverWith { case e => action.flatMap(_ => Execution.fromFuture(_ => Future.failed(e))) }

  /**
    * Applies the "during" action, calling "after" regardless of whether there was an error.
    * All errors are rethrown. Generalizes try/finally.
    */
  def bracket[B, C](after: A => Execution[B])(during: A => Execution[C]): Execution[C] = for {
    a <- execution
    r <- during(a) onException after(a)
    _ <- after(a)
  } yield r

  /** Like "bracket", but takes only a computation to run afterward. Generalizes "finally". */
  def ensuring[B](sequel: Execution[B]): Execution[A] = for {
    r <- onException(sequel)
    _ <- sequel
  } yield r
}

/** Pimps the Execution object. */
case class RichExecutionObject(exec: Execution.type) {
  /** Changes from HDFS context to Execution context. */
  def fromHdfs[T](hdfs: Hdfs[T]): Execution[T] = {
    /* Need to get the stack trace outside of the execution so we get a stack trace that is from the
     * main thread which includes the stack that called this function.
     */
    val stacktrace = Thread.currentThread.getStackTrace.tail

    Execution.getConfig.flatMap(config =>
      Execution.fromFuture(_ =>
        resultToFuture(hdfs.run(ConfHelper.getHadoopConf(config)), "HDFS operation failed", stacktrace)
      )
    )
  }

  /**
    * Changes from Hive context to Execution context.
    *
    * `modifyConf` can be used to update the hive conf that will be used to run the Hive action.
    */
  def fromHive[T](hive: Hive[T], modifyConf: HiveConf => Unit = _ => ()): Execution[T] = {
    /* Need to get the stack trace outside of the execution so we get a stack trace that is from the
     * main thread which includes the stack that called this function.
     */
    val stacktrace = Thread.currentThread.getStackTrace.tail

    Execution.getConfig.flatMap { config =>
      val hiveConf = new HiveConf(ConfHelper.getHadoopConf(config), this.getClass)
      modifyConf(hiveConf)
      Execution.fromFuture[T](_ =>
        resultToFuture(hive.run(hiveConf), "Hive operation failed", stacktrace)
      )
    }
  }

  /** Changes from an action that produces a Result to an Execution. */
  def fromResult[T](result: => Result[T]): Execution[T] = {
    /* Need to get the stack trace outside of the execution so we get a stack trace that is from the
     * main thread which includes the stack that called this function.
     */
    val stacktrace = Thread.currentThread.getStackTrace.tail

    Execution.fromFuture[T](_ => resultToFuture(result, "Operation failed", stacktrace))
  }

  /** Changes from an action that produces a scalaz Disjunction to an Execution. */
  def fromEither[T](disjunction: => String \/ T): Execution[T] =
    fromResult(disjunction.fold(Result.fail, Result.ok))

  /**
    * Helper function to convert a result to a future.
    *
    * For errors it creates a top level exception and fills it in with the provided stack trace.
    * It also prefixes the exception message with the provided prefix.
    */
  def resultToFuture[T](
    result: Result[T], prefix: String, stacktrace: Array[StackTraceElement]
  ): Future[T] =
    result.fold(
      Future.successful,
      error => {
        val (msg, cause) = error match {
          case This(msg)     => (s"$prefix: $msg"            , null)
          case That(ex)      => (s"$prefix: ${ex.getMessage}", ex)
          case Both(msg, ex) => (s"$prefix: $msg"            , ex)
        }
        val exception = new Exception(msg, cause)
        exception.setStackTrace(stacktrace)
        Future.failed(exception)
      }
    )
}

object ExecutionOps extends ExecutionOps

trait ExecutionOps {
  /** Implicit conversion of an Execution instance to RichExecution. */
  implicit def executionToRichExecution[A](execution: Execution[A]): RichExecution[A] =
    RichExecution[A](execution)

  /** Implicit conversion of Execution Object to RichExecutionObject. */
  implicit def ExecutionToRichExecution(exec: Execution.type): RichExecutionObject =
    RichExecutionObject(exec)

  implicit val scalazExecutionMonad: Monad[Execution] = new Monad[Execution] {
    def point[A](v: => A) = Execution.from(v)
    def bind[A, B](a: Execution[A])(f: A => Execution[B]) = a.flatMap(f)
  }
}
