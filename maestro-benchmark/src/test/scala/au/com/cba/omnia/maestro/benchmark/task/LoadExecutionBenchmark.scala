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

package au.com.cba.omnia.maestro.benchmark.task

import java.io.Serializable

import java.util.concurrent.{ExecutorService, Executors}

import scala.concurrent.{Await, ExecutionContext}
import scala.concurrent.duration._

import scala.util.Try

import org.scalameter.api.{PerformanceTest, Gen}

import com.twitter.scalding.{Execution, Config, Mode, Test, Stat}
import com.twitter.scalding.typed.{TypedPipe, IterablePipe}

import au.com.cba.omnia.maestro.core.task.{RawRow, LoadEx, LoadConfig}

import au.com.cba.omnia.maestro.benchmark.thrift.{Struct10, Struct20, Struct30, Implicits, Generators}

object Data extends Serializable {
  val strict10Pipes: Gen[TypedPipe[RawRow]] =
    Generators.struct10Rows.map(rows =>
      IterablePipe[RawRow](rows.map(row => RawRow(row.mkString("|"), List("2015-04-17"))))
    )

  val runner: Gen[Runner] =
    Gen.unit("runner").map(_ => Runner.inMemory)

  val struct10Data: Gen[(TypedPipe[RawRow], Runner)] =
    strict10Pipes zip runner
}

object LoadExecutionBenchmark extends PerformanceTest.OfflineReport {
  import Implicits._

  performance of "LoadEx" in {
    measure method "parseRows" in {
      using (Data.struct10Data)
      //.tearDown { case (_, runner) => runner.shutdown }
      .in { case (pipe, runner) => {
        val conf = LoadConfig[Struct10](errors = null) // parseRows does not write to any file, including errors
        val stat = NoOpStat
        val exec = LoadEx.parseRows(conf, stat, pipe).toIterableExecution
        val out  = runner.run(exec)

        // cycle through list as quickly as we can,
        // in case this is required to force any computations
        val it = out.iterator
        while (it.hasNext) { it.next }
      }}
    }
  }
}

case object NoOpStat extends Stat {
  def incBy(amount: Long) {}
}

case class Runner(config: Config, mode: Mode, es: ExecutorService) {
  def cec: ExecutionContext =
    ExecutionContext.fromExecutorService(es)

  def run[T](ex: Execution[T]): T =
    Await.result(ex.run(config, mode)(cec), 60 seconds)

  def shutdown() {
    es.shutdown
  }
}

object Runner {
  /** Creates a runner which only makes sense on Executions that do no IO */
  def inMemory = {
    val es     = Executors.newCachedThreadPool
    val mode   = Test(_ => None)
    val config = Config.from(Map(
      "jobclient.completion.poll.interval" -> "10",
      "cascading.flow.job.pollinginterval" -> "2"
    ))
    Runner(config, mode, es)
  }
}
