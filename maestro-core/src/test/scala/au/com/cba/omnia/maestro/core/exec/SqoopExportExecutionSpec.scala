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

package au.com.cba.omnia.maestro.core.exec

import java.util.UUID

import scala.util.Failure
import scala.io.Source

import au.com.cba.omnia.parlour.SqoopSyntax.ParlourExportDsl

import au.com.cba.omnia.thermometer.core.Thermometer._
import au.com.cba.omnia.thermometer.core.ThermometerSpec

import au.com.cba.omnia.maestro.core.exec.ParlourInstances._
import au.com.cba.omnia.maestro.core.task.CustomerExport

object SqoopExportExecutionSpec
  extends ThermometerSpec
  with SqoopExecution { def is = s2"""
  Sqoop Export Execution test
  ===========================

  Exporting data from HDFS to DB appending to existing rows  $endToEndExportWithAppend
  Export data from HDFS to DB deleting all existing rows     $endToEndExportWithDeleteTest
  Fails if sqlQuery set and need to delete all existing rows $endToEndExportWithQuery

"""
  val connectionString = "jdbc:hsqldb:mem:sqoopdb"
  val username         = "sa"
  val password         = ""
  val mapRedHome       = s"${System.getProperty("user.home")}/.ivy2/cache"
  val exportDir        = s"$dir/user/sales/books/customers/export"
  val resourceUrl      = getClass.getResource("/sqoop")
  val newCustomers     = Source.fromFile(s"${resourceUrl.getPath}/sales/books/customers/export/new-customers.txt").getLines().toList
  val oldCustomers     = Source.fromFile(s"${resourceUrl.getPath}/sales/books/customers/old-customers.txt").getLines().toList

  def options(table: String) = SqoopExportConfig.options[ParlourExportDsl](
    connectionString, username, password, table
  )

  SqoopExecutionTest.setupEnv()

  def endToEndExportWithAppend = {
    val table = s"customer_export_${UUID.randomUUID.toString.replace('-', '_')}"
    CustomerExport.tableSetup(connectionString, username, password, table, Option(oldCustomers))

    withEnvironment(path(resourceUrl.toString)) {
      val config = SqoopExportConfig(options(table))
      executesOk(sqoopExport(config, exportDir))
      CustomerExport.tableData(connectionString, username, password, table) must containTheSameElementsAs(newCustomers ++ oldCustomers)
    }
  }

  def endToEndExportWithDeleteTest = {
    val table = s"customer_export_${UUID.randomUUID.toString.replace('-', '_')}"
    CustomerExport.tableSetup(connectionString, username, password, table, Option(oldCustomers))

    withEnvironment(path(resourceUrl.toString)) {
      val config = SqoopExportConfig(options(table), deleteFromTable = true)
      executesOk(sqoopExport(config, exportDir))
      CustomerExport.tableData(connectionString, username, password, table) must containTheSameElementsAs(newCustomers)
    }
  }

  def endToEndExportWithQuery = {
    val table = s"customer_export_${UUID.randomUUID.toString.replace('-', '_')}"
    CustomerExport.tableSetup(connectionString, username, password, table, Option(oldCustomers))

    withEnvironment(path(resourceUrl.toString)) {
      val config = SqoopExportConfig(
        options(table).sqlQuery(s"select * from $table"), deleteFromTable = true
      )
      execute(sqoopExport(config, exportDir)) must throwA[RuntimeException](
        message = "SqoopOptions.getSqlQuery must be empty on Sqoop Export with delete from table"
      )
    }
  }
}
