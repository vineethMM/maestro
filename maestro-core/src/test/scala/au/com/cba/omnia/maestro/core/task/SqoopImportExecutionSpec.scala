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
import java.util.UUID

import au.com.cba.omnia.parlour.SqoopSyntax.ParlourImportDsl
import au.com.cba.omnia.thermometer.fact.PathFactoids._
import au.com.cba.omnia.thermometer.core.Thermometer._
import org.apache.commons.compress.compressors.bzip2.BZip2CompressorInputStream

import org.specs2.specification.BeforeExample

import scalaz.effect.IO

import au.com.cba.omnia.thermometer.context.Context
import au.com.cba.omnia.thermometer.tools.Streams
import au.com.cba.omnia.thermometer.core.{ThermometerRecordReader, ThermometerSpec}

object SqoopImportExecutionSpec extends ThermometerSpec with BeforeExample { def is = s2"""
  Sqoop Import Execution test
  ===========================

  Importing data from DB to HDFS with Execution $endToEndImport

"""
  val connectionString = "jdbc:hsqldb:mem:sqoopdb"
  val username         = "sa"
  val password         = ""
  val mapRedHome       = s"${System.getProperty("user.home")}/.ivy2/cache"
  val importTableName  = s"customer_import_${UUID.randomUUID.toString.replace('-', '_')}"
  val sqoop            = new SqoopExecution {}
  val source           = "sales"
  val domain           = "books"
  val timePath         = (List("2014", "10", "10") mkString File.separator)

  val bzippedRecordReader =
    ThermometerRecordReader[String]((conf, path) => IO {
      val ctx = new Context(conf)
      val in = new BZip2CompressorInputStream(
        ctx.withFileSystem[InputStream](_.open(path))("bzippedRecordReader")
      )
      try Streams.read(in).lines.toList
      finally in.close
    })

  def endToEndImport = {
    val importOptions: ParlourImportDsl = sqoop.createSqoopImportOptions(connectionString, username,
        password, importTableName, '|', "", Some("1=1")).splitBy("id").hadoopMapRedHome(mapRedHome)
    val execution = sqoop.sqoopImport(s"$dir/user/hdfs", source, domain, timePath, importOptions)
    val (path, count) = executesSuccessfully(execution)
    facts(s"$dir/user/hdfs/archive/sales/books" </> importTableName </> "2014/10/10" </> "part-00000.bz2" ==>
      records(bzippedRecordReader, CustomerImport.data))
    count === 3
  }



  override def before: Any = CustomerImport.tableSetup(connectionString, username, password, importTableName)
}

