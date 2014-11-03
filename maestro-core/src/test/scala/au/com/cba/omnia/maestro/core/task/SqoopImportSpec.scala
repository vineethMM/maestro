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

import java.io.{File, InputStream}

import org.apache.commons.compress.compressors.bzip2.BZip2CompressorInputStream

import scalaz.effect.IO

import com.twitter.scalding.{Args, CascadeJob}

import org.specs2.specification.BeforeExample

import scalikejdbc._

import au.com.cba.omnia.parlour.SqoopSyntax.ParlourImportDsl

import au.com.cba.omnia.thermometer.context.Context
import au.com.cba.omnia.thermometer.core.Thermometer._
import au.com.cba.omnia.thermometer.core.{ThermometerRecordReader, ThermometerSpec}
import au.com.cba.omnia.thermometer.fact.PathFactoids._
import au.com.cba.omnia.thermometer.tools.Streams

class SqoopImportSpec extends ThermometerSpec with BeforeExample { def is = s2"""
  Sqoop Import Cascade test
  =========================

  Import/Upload data from DB to HDFS $endToEndImportTest

"""
  val connectionString = "jdbc:hsqldb:mem:sqoopdb"
  val username = "sa"
  val password = ""
  val userHome = System.getProperty("user.home")

  def endToEndImportTest = {
    val cascade = withArgs(
      Map(
        "hdfs-root"        -> s"$dir/user/hdfs",
        "source"           -> "sales",
        "domain"           -> "books",
        "importTableName"  -> "customer_import",
        "timePath"         -> (List("2014", "10", "10") mkString File.separator),
        "mapRedHome"       -> s"$userHome/.ivy2/cache",
        "connectionString" -> connectionString,
        "username"         -> username,
        "password"         -> password
      )
    )(new SqoopImportCascade(_))

    val root = dir </> "user" </> "hdfs"
    val tail = "sales" </> "books" </> "customer_import" </> "2014" </> "10" </> "10"
    val dstDir = "source" </> tail
    val archiveDir = "archive" </> tail
    cascade.withFacts(
      root </> dstDir     </> "_SUCCESS"       ==> exists,
      root </> dstDir     </> "part-m-00000"   ==> lines(CustomerImport.data),
      root </> archiveDir </> "_SUCCESS"       ==> exists,
      root </> archiveDir </> "part-00000.bz2" ==> records(bzippedRecordReader, CustomerImport.data)
    )
  }

  override def before: Any = CustomerImport.tableSetup(connectionString, username, password)

  val bzippedRecordReader =
    ThermometerRecordReader[String]((conf, path) => IO {
      val ctx = new Context(conf)
      val in = new BZip2CompressorInputStream(
        ctx.withFileSystem[InputStream](_.open(path))("bzippedRecordReader")
      )
      try Streams.read(in).lines.toList
      finally in.close
    })
}

class SqoopImportCascade(args: Args) extends CascadeJob(args) with Sqoop {
  val hdfsRoot         = args("hdfs-root")
  val source           = args("source")
  val domain           = args("domain")
  val importTableName  = args("importTableName")
  val mappers          = 1
  val database         = domain
  val connectionString = args("connectionString")
  val username         = args("username")
  val password         = args("password")
  val timePath         = args("timePath")
  val mapRedHome       = args("mapRedHome")

  /**
   * hadoopMapRedHome is set for Sqoop to find the hadoop jars. This hack would be necessary ONLY in a
   * test case.
   */
  val importOptions: ParlourImportDsl = createSqoopImportOptions(connectionString, username,
    password, importTableName, '|', "", Some("1=1")).splitBy("id").hadoopMapRedHome(mapRedHome)

  override val jobs = sqoopImport(hdfsRoot, source, domain, timePath, importOptions)(args)._1
}

object CustomerImport {

  Class.forName("org.hsqldb.jdbcDriver")

  val data = List("1|Fred|001|D|M|259", "2|Betty|005|D|M|205", "3|Bart|002|F|M|225")

  def tableSetup(connectionString: String, username: String, password: String): Unit = {
    ConnectionPool.singleton(connectionString, username, password)
    implicit val session = AutoSession

    sql"""
      create table customer_import (
        id integer,
        name varchar(20),
        accr varchar(20),
        cat varchar(20),
        sub_cat varchar(20),
        balance integer
      )
    """.execute.apply()

    data.map(line => line.split('|')).foreach(
      row => sql"""insert into customer_import(id, name, accr, cat, sub_cat, balance)
        values (${row(0)}, ${row(1)}, ${row(2)}, ${row(3)}, ${row(4)}, ${row(5)})""".update().apply())
  }
}
