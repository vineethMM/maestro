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

import java.io.File

import org.joda.time.{DateTime, DateTimeZone}

import com.twitter.scalding.{Config, Execution}

import au.com.cba.omnia.parlour.ParlourImportOptions

import au.com.cba.omnia.maestro.api.exec._
import au.com.cba.omnia.maestro.api.exec.Maestro._
import au.com.cba.omnia.maestro.example.thrift.Customer

/** Configuration for `CustomerSqoopImportExecution` */
case class CustomerImportConfig(config: Config) extends MacroSupport[Customer] {
  val maestro     = MaestroConfig(
    conf          = config,
    source        = "sales",
    domain        = "books",
    tablename     = "customer_import"
  )
  val sqoopImport  = maestro.sqoopImport(
    initialOptions = Some(ParlourImportDsl().splitBy("id"))
  )
  val load        = maestro.load(
    none          = "null"
  )
  val catTable    = maestro.partitionedHiveTable[Customer, String](
    partition     = Partition.byField(Fields.Cat),
    tablename     = "by_cat"
  )
}

/** Example customer execution, importing data via Sqoop */
object CustomerSqoopImportExecution extends MacroSupport[Customer] {
  /** Create an example customer sqoop import execution */
  def execute: Execution[CustomerImportStatus] = for {
    conf               <- Execution.getConfig.map(CustomerImportConfig(_))
    (path, sqoopCount) <- sqoopImport(conf.sqoopImport)
    (pipe, loadInfo)   <- load[Customer](conf.load, List(path))
    loadSuccess        <- loadInfo.withSuccess
    hiveCount          <- viewHive(conf.catTable, pipe)
  } yield (CustomerImportStatus(sqoopCount, loadSuccess.written, hiveCount))
}

case class CustomerImportStatus(sqoopCount: Long, loadCount: Long, hiveCount: Long)
