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

package au.com.cba.omnia.maestro.core
package task

import au.com.cba.omnia.thermometer.core.{ThermometerSpec, Thermometer}, Thermometer._

import au.com.cba.omnia.maestro.core.clean.Clean
import au.com.cba.omnia.maestro.core.codec.{Decode, Tag}
import au.com.cba.omnia.maestro.core.data.Field
import au.com.cba.omnia.maestro.core.filter.RowFilter
import au.com.cba.omnia.maestro.core.validate.Validator

import au.com.cba.omnia.maestro.core.thrift.scrooge.StringPair

private object LoadExec extends LoadExecution

object LoadExecutionSpec extends ThermometerSpec with LoadTestUtil { def is = s2"""

Load execution properties
=========================

  can load using execution monad                                 $normal
  can load fixed length using execution monad                    $fixed
  returns the right load info for no data                        $noData
  returns the right load info for an acceptable number of errors $someErrors
  returns the right load info for too many errors                $manyErrors

"""

  def normal = {
    withEnvironment(path(getClass.getResource("/load-execution").toString)) {
      executesSuccessfully(LoadExec.load[StringPair](
        "|", List("normal"), "errors", now, clean, validator, RowFilter.keep, "null"
      ))._2 must_== LoadSuccess(4, 4, 0)
    }
  }

  def fixed = {
    withEnvironment(path(getClass.getResource("/load-execution").toString)) {
      executesSuccessfully(LoadExec.loadFixedLength[StringPair](
        List(4), List("fixed"), "errors", now, clean, validator, RowFilter.keep, "null"
      ))._2 must_== LoadSuccess(4, 4, 0)
    }
  }

  def noData = {
    withEnvironment(path(getClass.getResource("/load-execution").toString)) {
      executesSuccessfully(LoadExec.load[StringPair](
        "|", List("no-data"), "errors", now, clean, validator, RowFilter.keep, "null"
      ))._2 must_== EmptyLoad
    }
  }

  def someErrors = {
    withEnvironment(path(getClass.getResource("/load-execution").toString)) {
      executesSuccessfully(LoadExec.load[StringPair](
        "|", List("some-errors"), "errors", now, clean, validator, RowFilter.keep, "null", 0.25
      ))._2 must_== LoadSuccess(5, 4, 1)
    }
  }

  def manyErrors = {
    withEnvironment(path(getClass.getResource("/load-execution").toString)) {
      executesSuccessfully(LoadExec.load[StringPair](
        "|", List("many-errors"), "errors", now, clean, validator, RowFilter.keep, "null", 0.25
      ))._2 must_== LoadFailure(5, 3, 2)
    }
  }
}
