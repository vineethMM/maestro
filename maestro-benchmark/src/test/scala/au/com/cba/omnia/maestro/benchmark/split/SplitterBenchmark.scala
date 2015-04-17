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

package au.com.cba.omnia.maestro.benchmark.split

import java.io.Serializable

import org.scalameter.api.{PerformanceTest, Gen}

import au.com.cba.omnia.maestro.core.split.Splitter

import au.com.cba.omnia.maestro.benchmark.thrift.Generators

object SplitterBenchmark extends PerformanceTest.OfflineReport {
  def testDelimited(rows: Gen[Array[List[String]]]) =
    using (rows.map(_.map(_.mkString("|")))) in { rows => {
      val splitter = Splitter.delimited("|")
      var i = 0
      while (i < rows.length) {
        splitter.run(rows(i))
        i = i+1
      }
    }}

  performance of "Splitter" in {
    measure method "delimited[Struct10]" in testDelimited(Generators.struct10Rows)
    measure method "delimited[Struct20]" in testDelimited(Generators.struct20Rows)
    measure method "delimited[Struct30]" in testDelimited(Generators.struct30Rows)
  }
}
