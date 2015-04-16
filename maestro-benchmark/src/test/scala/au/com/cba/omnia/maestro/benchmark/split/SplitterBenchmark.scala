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

import scala.util.Random

import org.scalameter.api.{PerformanceTest, Gen}

import au.com.cba.omnia.maestro.core.split.Splitter

object Data extends Serializable {
  def mkDelimited(rows: Int, fields: Int, delimiter: String): Array[String] =
    (0 until rows).map(row =>
      (0 until fields).map(field => s"r$row-f$field").mkString(delimiter)
    ).toArray

  val rows   = Gen.range("rows")(10000, 30000, 10000)
  val fields = Gen.range("fields")(10, 30, 10)

  def delimited(delimiter: String): Gen[Array[String]] = for {
    numRows   <- rows
    numFields <- fields
  } yield mkDelimited(numRows, numFields, delimiter)
}

object SplitterBenchmark extends PerformanceTest.OfflineReport {
  performance of "Splitter" in {
    measure method "delimited" in {
      using (Data.delimited("|")) in { rows => {
        val splitter = Splitter.delimited("|")
        var i = 0
        while (i < rows.length) {
          splitter.run(rows(i))
          i = i+1
        }
      }}
    }
  }

}
