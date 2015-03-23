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

import scala.util.Failure

import scalaz._, Scalaz._

import com.twitter.scalding.Execution

import org.specs2.matcher.Matcher

import au.com.cba.omnia.thermometer.core.ThermometerSpec
import au.com.cba.omnia.thermometer.hive.HiveSupport

import au.com.cba.omnia.omnitool.Result

import au.com.cba.omnia.permafrost.hdfs.Hdfs

import au.com.cba.omnia.ebenezer.scrooge.hive.Hive

import ExecutionOps._

object Executions {
  def hive   = Execution.fromHive(Hive.value(throw new Exception("test")))
  def hdfs   = Execution.fromHdfs(Hdfs.value(throw new Exception("test")))
  def result = Execution.fromResult(Result.fail("error"))
  def either = Execution.fromEither("error".left)
}

object RichExecutionSpec extends ThermometerSpec with HiveSupport { def is = s2"""

Rich Execution
==============

The RichExecution object should:
  provide useful exception information for `fromHive`   $hive
  provide useful exception information for `fromHdfs`   $hdfs
  provide useful exception information for `fromResult` $result
  provide useful exception information for `fromEither` $either

"""

  def hive   = Executions.hive   must beFailureWithClass(Executions)
  def hdfs   = Executions.hdfs   must beFailureWithClass(Executions)
  def result = Executions.result must beFailureWithClass(Executions)
  def either = Executions.either must beFailureWithClass(Executions, 2)

  def beFailureWithClass[A](clazz: Any, skip: Int = 1): Matcher[Execution[A]] =
    (execution: Execution[A]) => execute(execution) must beLike {
      case Failure(t) => t.getStackTrace()(skip).getClassName must_== clazz.getClass.getName
    }
}
