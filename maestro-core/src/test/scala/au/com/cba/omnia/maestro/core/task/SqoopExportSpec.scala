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

import scala.io.Source

import com.twitter.scalding.{Args, CascadeJob}

import org.specs2.specification.BeforeExample

import scalikejdbc._

import au.com.cba.omnia.parlour.SqoopSyntax.ParlourExportDsl

import au.com.cba.omnia.thermometer.core.Thermometer._
import au.com.cba.omnia.thermometer.core.ThermometerSpec

import au.com.cba.omnia.maestro.core.task.CustomerExport._

class SqoopExportSpec extends ThermometerSpec { def is = s2"""
  Sqoop Export Cascade test
  =========================

  Export data from HDFS to DB with appending rows            $endToEndExportTest
  Export data from HDFS to DB with deleting all rows first   $endToEndExportWithDeleteTest

"""
  val argsMap = Map(
    "exportDir"        -> s"$dir/user/sales/books/customers",
    "mapRedHome"       -> s"$userHome/.ivy2/cache",
    "connectionString" -> connectionString,
    "username"         -> username,
    "password"         -> password
  )

  val resourceUrl = getClass.getResource("/sqoop")

  val oldData = Seq((1, "Hugo", "abc_accr", "Fish", "Tuna", 200))
  val oldTextData = Seq("1|Hugo|abc_accr|Fish|Tuna|200")

  val newTextData = Source.fromFile(s"${resourceUrl.getPath}/sales/books/customers/customer.txt").getLines().toSeq

  def endToEndExportTest = {
    val table = s"table_${UUID.randomUUID.toString.replace('-', '_')}"
    tableSetup(table, oldData)

    withEnvironment(path(resourceUrl.toString)) {
      withArgs(argsMap + ("exportTableName"  -> table))(new SqoopExportCascade(_, false)).run
      tableData(table) must containTheSameElementsAs(newTextData ++ oldTextData)
    }
  }

  def endToEndExportWithDeleteTest = {
    val table = s"table_${UUID.randomUUID.toString.replace('-', '_')}"
    tableSetup(table, oldData)

    withEnvironment(path(resourceUrl.toString)) {
      withArgs(argsMap + ("exportTableName"  -> table))(new SqoopExportCascade(_, true)).run
      tableData(table) must containTheSameElementsAs(newTextData)
    }
  }
}

class SqoopExportCascade(args: Args, deleteFromTable: Boolean) extends CascadeJob(args) with Sqoop {
  val exportDir        = args("exportDir")
  val exportTableName  = args("exportTableName")
  val mappers          = 1
  val connectionString = args("connectionString")
  val username         = args("username")
  val password         = args("password")
  val mapRedHome       = args("mapRedHome")

  /**
   * hadoopMapRedHome is set for Sqoop to find the hadoop jars. This hack would be necessary ONLY in a
   * test case.
   */
  val exportOptions = ParlourExportDsl().hadoopMapRedHome(mapRedHome)

  override val jobs = Seq(sqoopExport(exportDir, exportTableName, connectionString, username, password, '|', "", exportOptions, deleteFromTable)(args))
}

object CustomerExport {

  Class.forName("org.hsqldb.jdbcDriver")

  val connectionString = "jdbc:hsqldb:mem:sqoopdb"
  val username = "sa"
  val password = ""
  val userHome = System.getProperty("user.home")

  implicit val session = AutoSession

  def tableSetup(table: String, data: Seq[(Int, String, String, String, String, Int)]): Unit = {
    ConnectionPool.singleton(connectionString, username, password)

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

    tableInsert(table, data)
  }

  def tableInsert(table: String, data: Seq[(Int, String, String, String, String, Int)]) = {
    data.map { case (id, name, accr, cat, sub_cat, balance) =>
      SQL(s"""
        insert into $table
        values (?, ?, ?, ?, ?, ?)
      """).bind(id, name, accr, cat, sub_cat, balance).update.apply()
    }
  }

  def tableData(table: String): List[String] = {
    ConnectionPool.singleton(connectionString, username, password)
    implicit val session = AutoSession
    SQL(s"select * from $table").map(rs => List(rs.int("id"), rs.string("name"), rs.string("accr"),
      rs.string("cat"), rs.string("sub_cat"), rs.int("balance")) mkString "|").list.apply()
  }
}


