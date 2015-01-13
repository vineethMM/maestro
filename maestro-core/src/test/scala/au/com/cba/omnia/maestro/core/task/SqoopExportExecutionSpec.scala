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

package au.com.cba.omnia.maestro.core.task

import java.util.UUID

import scala.util.Failure
import scala.io.Source

import scalikejdbc.{SQL, ConnectionPool, AutoSession}
import au.com.cba.omnia.parlour.SqoopSyntax.{ParlourExportDsl,TeradataParlourExportDsl}


import au.com.cba.omnia.thermometer.core.Thermometer._
import au.com.cba.omnia.thermometer.core.ThermometerSpec

object SqoopExportExecutionSpec
  extends ThermometerSpec
  with SqoopExecution { def is = s2"""
  Sqoop Export Execution test
  ===========================

  Exporting data from HDFS to DB appending to existing rows  $endToEndExportWithAppend
  Export data from HDFS to DB deleting all existing rows     $endToEndExportWithDeleteTest
  Fails if sqlQuery set and need to delete all existing rows $endToEndExportWithQuery
  Fails with Teradata connection manager                     $endToEndExportWithTeradataConnMan
  Succeeds with Teradata after connection manager reset      $endToEndExportWithTeradataResetConnMan

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

  Class.forName("org.hsqldb.jdbcDriver")

  SqoopExecutionTest.setupEnv()

  def endToEndExportWithAppend = {
    val table = s"customer_export_${UUID.randomUUID.toString.replace('-', '_')}"
    tableSetup(connectionString, username, password, table, Option(oldCustomers))

    withEnvironment(path(resourceUrl.toString)) {
      val config = SqoopExportConfig(options(table))
      executesOk(sqoopExport(config, exportDir))
      tableData(connectionString, username, password, table) must containTheSameElementsAs(newCustomers ++ oldCustomers)
    }
  }

  def endToEndExportWithDeleteTest = {
    val table = s"customer_export_${UUID.randomUUID.toString.replace('-', '_')}"
    tableSetup(connectionString, username, password, table, Option(oldCustomers))

    withEnvironment(path(resourceUrl.toString)) {
      val config = SqoopExportConfig(options(table), deleteFromTable = true)
      executesOk(sqoopExport(config, exportDir))
      tableData(connectionString, username, password, table) must containTheSameElementsAs(newCustomers)
    }
  }

  def endToEndExportWithQuery = {
    val table = s"customer_export_${UUID.randomUUID.toString.replace('-', '_')}"
    tableSetup(connectionString, username, password, table, Option(oldCustomers))

    withEnvironment(path(resourceUrl.toString)) {
      val config = SqoopExportConfig(
        options(table).sqlQuery(s"select * from $table"), deleteFromTable = true
      )
      execute(sqoopExport(config, exportDir)) must throwA[RuntimeException](
        message = "SqoopOptions.getSqlQuery must be empty on Sqoop Export with delete from table"
      )
    }
  }

  def TeradataOptions(table: String) = SqoopExportConfig.options[TeradataParlourExportDsl](
    connectionString, username, password, table
  )

  def endToEndExportWithTeradataConnMan = {
    SqoopExecutionTest.setupEnv()
    val table = s"customer_export_${UUID.randomUUID.toString.replace('-', '_')}"
    tableSetup(connectionString, username, password, table, Option(List()))

    withEnvironment(path(resourceUrl.toString)) {
      val config = SqoopExportConfig(TeradataOptions(table))
      execute(sqoopExport(config, exportDir)) must beLike { case Failure(_) => ok }
    }
  }

  def endToEndExportWithTeradataResetConnMan = {
    SqoopExecutionTest.setupEnv(customConnMan=Some(""))
    val table = s"customer_export_${UUID.randomUUID.toString.replace('-', '_')}"
    tableSetup(connectionString, username, password, table, Option(List()))

    withEnvironment(path(resourceUrl.toString)) {
      val config = SqoopExportConfig(TeradataOptions(table))
      executesOk(sqoopExport(config, exportDir))
      tableData(connectionString, username, password, table) must containTheSameElementsAs(newCustomers)
    }
  }

  def tableSetup(
    connectionString: String,
    username: String,
    password: String,
    table: String = "customer_export",
    data: Option[List[String]] = None
  ): Unit = {
    ConnectionPool.singleton(connectionString, username, password)
    implicit val session = AutoSession
    SQL(s"""
      create table $table (
        id integer,
        name varchar(20),
        accr varchar(20),
        cat varchar(20),
        sub_cat varchar(20),
        balance integer
      )
    """).execute.apply()

    data.foreach(lines =>
      lines.foreach(line => {
        val row = line.split('|')
        SQL(s"""insert into ${table}(id, name, accr, cat, sub_cat, balance)
          values ('${row(0)}', '${row(1)}', '${row(2)}', '${row(3)}', '${row(4)}', '${row(5)}')"""
        ).update().apply()
      })
    )
  }

  def tableData(
    connectionString: String,
    username: String,
    password: String,
    table: String = "customer_export"
  ): List[String] = {
    ConnectionPool.singleton(connectionString, username, password)
    implicit val session = AutoSession
    SQL(s"select * from $table").map(rs => List(rs.int("id"), rs.string("name"), rs.string("accr"),
      rs.string("cat"), rs.string("sub_cat"), rs.int("balance")) mkString "|").list.apply()
  }
}
