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

import com.twitter.scalding.{Config, Execution}

import org.apache.hadoop.hive.conf.HiveConf

import au.com.cba.omnia.permafrost.hdfs.Hdfs

import au.com.cba.omnia.ebenezer.scrooge.hive.Hive

/** Pimps an Execution instance. */
case class RichExecution[A](execution: Execution[A]) {
  def withSubConfig(modifyConfig: Config => Config): Execution[A] =
    Execution.getConfigMode.flatMap { case (config, mode) =>
      Execution.fromFuture(cec => execution.run(modifyConfig(config), mode)(cec))
    }
}

/** Pimps the Execution object. */
case class RichExecutionObject(exec: Execution.type) {
  /** Changes from HDFS context to Execution context. */
  def fromHdfs[T](hdfs: Hdfs[T]): Execution[T] = {
    Execution.getConfig.flatMap { config =>
      Execution.fromFuture(_ => hdfs.run(ConfHelper.getHadoopConf(config)).foldAll(
        x         => Future.successful(x),
        msg       => Future.failed(new Exception(msg)),
        ex        => Future.failed(ex),
        (msg, ex) => Future.failed(new Exception(msg, ex))
      ))
    }
  }

  /**
    * Changes from Hive context to Execution context.
    *
    * `modifyConf` can be used to update the hive conf that will be used to run the Hive action.
    */
  def fromHive[T](hive: Hive[T], modifyConf: HiveConf => Unit = _ => ()): Execution[T] = {
    Execution.getConfig.flatMap { config =>
      val hiveConf = new HiveConf(ConfHelper.getHadoopConf(config), this.getClass)
      modifyConf(hiveConf)
      Execution.fromFuture(_ => hive.run(hiveConf).foldAll(
        x         => Future.successful(x),
        msg       => Future.failed(new Exception(msg)),
        ex        => Future.failed(ex),
        (msg, ex) => Future.failed(new Exception(msg, ex))
      ))
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
