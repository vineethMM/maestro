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

import scalaz.\&/.{This, That, Both}

import com.twitter.scalding.{Config, Execution}

import au.com.cba.omnia.omnitool.{Error, Ok, Result}

import au.com.cba.omnia.permafrost.hdfs.Hdfs

/** Pimps an Execution instance. */
case class RichExecution[A](execution: Execution[A]) {
  def withSubConfig(modifyConfig: Config => Config): Execution[A] =
    Execution.getConfigMode.flatMap { case (config, mode) =>
      Execution.fromFuture(cec => execution.run(modifyConfig(config), mode)(cec))
    }
}

/** Pimps the Execution object. */
case class RichExecutionObject(exec: Execution.type) {
  /** Changes from  HDFS context to Execution context. */
  def fromHdfs[T](hdfs: Hdfs[T]): Execution[T] = {
    Execution.getConfig.flatMap { config =>
      Execution.fromFuture(_ => hdfs.run(ConfHelper.getHadoopConf(config)) match {
        case Ok(x)             => Future.successful(x)
        case Error(This(s))    => Future.failed(new Exception(s))
        case Error(That(e))    => Future.failed(e)
        case Error(Both(s, e)) => Future.failed(new Exception(s, e))
      })
    }
  }
}

object ExecutionOps extends ExecutionOps

trait ExecutionOps {
  /** Implicit conversion of an Execution instance to RichExecution. */
  implicit def executionToRichExecution[A](execution: Execution[A]): RichExecution[A] =
    RichExecution[A](execution)

  /** Implicit conversion of Execution Object to RichExecutionObject. */
  implicit def ExecutionToRichExecution(exec: Execution.type): RichExecutionObject =
    RichExecutionObject(exec)

}
