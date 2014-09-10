
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

import com.twitter.scalding.Args

import scalaz.{Tag => _, _}, Scalaz._

import au.com.cba.omnia.maestro.api.{MaestroCascade, Partition, RowFilter, UniqueJob, Validator}
import au.com.cba.omnia.maestro.api.Clean
import au.com.cba.omnia.maestro.api.Maestro.{load, now, view}
import au.com.cba.omnia.maestro.core.task.Sqoop
import au.com.cba.omnia.maestro.example.thrift.Customer
import au.com.cba.omnia.parlour.SplitByAmp
import au.com.cba.omnia.parlour.SqoopSyntax.TeradataParlourImportDsl


class CustomerSqoopCascade(args: Args) extends MaestroCascade[Customer](args) with Sqoop {
  val hdfsRoot = args("hdfs-root")
  val source = args("source")
  val domain = args("domain")
  val tablename = args("tableName")
  val mappers = args("mappers").toInt
  val host = args("host")
  val database = domain
  val connectionString = s"jdbc:teradata://${host}/MODE=ANSI,CHARSET=UTF8,DATABASE=${database}"
  val username = args("username")
  val password = args("password")

  val errors = s"${hdfsRoot}/errors/${domain}"
  val filter = RowFilter.keep
  val cleaners = Clean.all(
    Clean.trim,
    Clean.removeNonPrintables)
  val validators = Validator.all[Customer]()
  val customerView = s"${hdfsRoot}/view/warehouse/${domain}/${tablename}"

  /**
   * In order for sqoop to work with Teradata, you will need to include the teradata drivers and cloudera connector 
   * in the maestro-example/lib folder when building the assembly. 
   */
  
  val sourceDir = "/data/things/bla"
  val importOptions = TeradataParlourImportDsl()
    .numberOfMappers(mappers)
    .inputMethod(SplitByAmp)
    .tableName(tablename)
    .targetDir(sourceDir)
    .connectionString(connectionString)
    .username(username)
    .password(password)
    .splitBy("id")
  importOptions.options.setSkipDistCache(true)

  //val (sqoopJobs, imported) = sqoopImport(hdfsRoot, source, domain, tablename, connectionString, username, password, importOptions)(args)
  System.setProperty("sqoop.throwOnError", "true")
  val doThings = new UniqueJob(args) {
    load[Customer]("|", List(sourceDir), errors, now(), cleaners, validators, filter, "null") |>
    view(Partition.byField(Fields.Cat), customerView)
  }
  val sqoopJob = customSqoopImport2(doThings, importOptions)(args)

  println(sqoopJob.buildFlow.getSinkNames())
  println(doThings.buildFlow.getSourceNames())
  
  override val jobs = Seq(sqoopJob, doThings)
}


