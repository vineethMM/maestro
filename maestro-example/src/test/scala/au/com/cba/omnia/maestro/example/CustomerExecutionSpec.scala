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

package au.com.cba.omnia.maestro.example

import au.com.cba.omnia.thermometer.core.{ThermometerSpec, Thermometer}, Thermometer._
import au.com.cba.omnia.thermometer.hive.HiveSupport
import au.com.cba.omnia.thermometer.fact.PathFactoids._

import au.com.cba.omnia.ebenezer.test.ParquetThermometerRecordReader

import au.com.cba.omnia.maestro.api._
import au.com.cba.omnia.maestro.test.Records
import au.com.cba.omnia.maestro.core.task.LoadSuccess

import au.com.cba.omnia.maestro.example.thrift.Customer

object CustomerExecutionSpec extends ThermometerSpec with Records with HiveSupport { def is = s2"""

Customer Execution
==================

  end to end pipeline        $pipeline

"""

  lazy val decoder = Macros.mkDecode[Customer]

  def pipeline = {
    val actualReader   = ParquetThermometerRecordReader[Customer]
    val expectedReader = delimitedThermometerRecordReader[Customer]('|', "null", decoder)

    withEnvironment(path(getClass.getResource("/customer").toString)) {
      executesSuccessfully(CustomerExecution.execute(s"$dir/user", s"$dir/user", s"$dir/user/archive")) must_== ((
        LoadSuccess(8, 8, 8, 0),
        8
      ))

      facts(
        hiveWarehouse </> "customer.db" </> "by_date" ==> recordsByDirectory(actualReader, expectedReader, "expected" </> "customer" </> "by-date"),
        hiveWarehouse </> "customer.db" </> "by_cat"  ==> recordsByDirectory(actualReader, expectedReader, "expected" </> "customer" </> "by-cat")
      )
    }
  }
}
