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

import org.apache.commons.compress.compressors.gzip.GzipCompressorInputStream

import scalaz.effect.IO

import com.twitter.scalding.{Args, CascadeJob}

import au.com.cba.omnia.parlour.SqoopSyntax.ParlourImportDsl

import au.com.cba.omnia.thermometer.context.Context
import au.com.cba.omnia.thermometer.core.Thermometer._
import au.com.cba.omnia.thermometer.core.{ThermometerRecordReader, ThermometerSpec}
import au.com.cba.omnia.thermometer.fact.PathFactoids._
import au.com.cba.omnia.thermometer.tools.Streams

class SqoopImportSpec extends ThermometerSpec { def is = s2"""
  Sqoop Import Cascade test
  =========================

  Import/Upload data from DB to HDFS            $endToEndImportTest
  Import with query/Upload data from DB to HDFS $endToEndImportWithQueryTest

"""
  val connectionString = "jdbc:hsqldb:mem:sqoopdb"
  val username = "sa"
  val password = ""
  val userHome = System.getProperty("user.home")

  def endToEndImportTest = endToEndImport(
    new SqoopImportCascade(_),
    "customer_import",
    CustomerImport.data
  )


  val filteredData = CustomerImport.dataSplitted.filter(_(5).toInt > 220).map(_.mkString("|"))
  def endToEndImportWithQueryTest = endToEndImport(
    new SqoopImportWithQueryCascade(_),
    "customer_import_with_query",
    filteredData
  )

  def endToEndImport(cascadeFactory: Args => CascadeJob, tableName: String, expectedRows: List[String]) = {
    CustomerImport.tableSetup(connectionString, username, password, tableName)
    val cascade = withArgs(
      Map(
        "hdfs-root"        -> s"$dir/user/hdfs",
        "source"           -> "sales",
        "domain"           -> "books",
        "importTableName"  -> tableName,
        "timePath"         -> (List("2014", "10", "10") mkString File.separator),
        "mapRedHome"       -> s"$userHome/.ivy2/cache",
        "connectionString" -> connectionString,
        "username"         -> username,
        "password"         -> password
      )
    )(cascadeFactory(_))

    val root = dir </> "user" </> "hdfs"
    val tail = "sales" </> "books" </> tableName </> "2014" </> "10" </> "10"
    val dstDir = "source" </> tail
    val archiveDir = "archive" </> tail
    cascade.withFacts(
      root </> dstDir     </> "_SUCCESS"       ==> exists,
      root </> dstDir     </> "part-m-00000"   ==> lines(expectedRows),
      root </> archiveDir </> "_SUCCESS"       ==> exists,
      root </> archiveDir </> "part-00000.gz"  ==> records(compressedRecordReader, expectedRows)
    )
  }

  val compressedRecordReader =
    ThermometerRecordReader[String]((conf, path) => IO {
      val ctx = new Context(conf)
      val in = new GzipCompressorInputStream(
        ctx.withFileSystem[InputStream](_.open(path))("compressedRecordReader")
      )
      try Streams.read(in).lines.toList
      finally in.close
    })
}

class SqoopImportCascade(args: Args) extends BaseSqoopImportCascade(args) {
  def importOptions: ParlourImportDsl = createSqoopImportOptions(connectionString, username,
    password, importTableName, '|', "", Some("1=1")
  ).splitBy("id")

  override def jobs = sqoopImport(hdfsRoot, source, domain, timePath, withMRHome(importOptions))(args)._1
}

class SqoopImportWithQueryCascade(args: Args) extends BaseSqoopImportCascade(args) {
  val importOptions: ParlourImportDsl = createSqoopImportOptionsWithQuery(connectionString, username,
    password, s"SELECT * FROM $importTableName WHERE balance > 220 AND $$CONDITIONS", Some("id"), '|', ""
  )

  override val jobs = sqoopImportWithQuery(hdfsRoot, source, domain, timePath, importTableName, withMRHome(importOptions))(args)._1
}

abstract class BaseSqoopImportCascade(args: Args) extends CascadeJob(args) with Sqoop {
  val hdfsRoot         = args("hdfs-root")
  val source           = args("source")
  val domain           = args("domain")
  val importTableName  = args("importTableName")
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
  def withMRHome(options: ParlourImportDsl) = options.hadoopMapRedHome(mapRedHome)
}


