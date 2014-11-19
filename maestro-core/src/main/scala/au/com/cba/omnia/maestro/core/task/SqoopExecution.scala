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

import java.io.File

import au.com.cba.omnia.maestro.core.scalding.StatKeys
import au.com.cba.omnia.parlour.{SqoopExecution => ParlourExecution, ParlourImportOptions}
import au.com.cba.omnia.parlour.SqoopSyntax.ParlourImportDsl
import cascading.flow.FlowDef
import com.cba.omnia.edge.source.compressible.CompressibleTypedTsv
import com.twitter.scalding._
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.io.compress.{BZip2Codec, CompressionCodec}
import org.apache.log4j.Logger

import scala.reflect._

trait SqoopExecution {

  /** Return an [[Execution]] that will run a custom sqoop import using parlour import options
    *
    * '''Use this method ONLY if non-standard settings are required'''
    *
    * @param options: Parlour import options
    */
  def customSqoopImport(options: ParlourImportOptions[_]): Execution[Unit] = {
    val logger = Logger.getLogger("Sqoop")
    val sqoopOptions = options.toSqoopOptions
    logger.info(s"connectionString = ${sqoopOptions.getConnectString}")
    logger.info(s"tableName        = ${sqoopOptions.getTableName}")
    logger.info(s"targetDir        = ${sqoopOptions.getTargetDir}")
    ParlourExecution.sqoopImport(sqoopOptions)
  }

  /**
   * Convenience method to populate a parlour import option instance
   *
   * @param connectionString: database connection string
   * @param username: database username
   * @param password: database password
   * @param dbTableName: Table name in database
   * @param outputFieldsTerminatedBy: output field terminating character
   * @param nullString: The string to be written for a null value in columns
   * @param whereCondition: where condition if any
   * @param options: parlour option to populate
   * @return : Populated parlour option
   */
  def createSqoopImportOptions[T <: ParlourImportOptions[T]](
    connectionString: String,
    username: String,
    password: String,
    dbTableName: String,
    outputFieldsTerminatedBy: Char,
    nullString: String,
    whereCondition: Option[String] = None,
    options: T = ParlourImportDsl()
  ): T = {
    SqoopHelper.createSqoopImportOptions[T](
      connectionString,
      username,
      password,
      dbTableName,
      outputFieldsTerminatedBy,
      nullString,
      whereCondition,
      options
    )
  }

  /**
   * Runs a sqoop import from a database table to HDFS.
   *
   * Data will be copied to a path that is generated. For a given `domain`,`tableName` and `timePath`, a path
   * `\$hdfsRoot/source/\$source/\$domain/\$tableName/\$timePath` is generated.
   *
   * Use [[Sqoop.createSqoopImportOptions]] to populate the [[ParlourImportOptions]] parameter.
   *
   * @param hdfsRoot: Root directory of HDFS
   * @param source: Source system
   * @param domain: Database within source
   * @param timePath: custom timepath to import data into
   * @param options: Sqoop import options
   * @param tableName: Table name used in the import path (if not set then options.tableName is used)
   * @return Tuple of import directory and number of rows imported
   */
  def sqoopImport[T <: ParlourImportOptions[T]](
    hdfsRoot: String,
    source: String,
    domain: String,
    timePath: String,
    options: T,
    tableName: Option[String] = None
  ): Execution[(String, Long)] = {
    for {
      dstTableName <- Execution.from {
        tableName.getOrElse(
          options.getTableName.getOrElse(throw new RuntimeException("Table name must be set in ParlourImportOptions"))
        )
      }
      importPath    = List(hdfsRoot, "source", source, domain, dstTableName, timePath) mkString File.separator
      archivePath   = List(hdfsRoot, "archive", source, domain, dstTableName, timePath) mkString File.separator
      finalOptions  = options.targetDir(importPath)
      _            <- customSqoopImport(finalOptions)
      count        <- Archiver.archive[BZip2Codec](importPath, archivePath)
    } yield((importPath, count))
  }
}

private object SqoopHelper extends Sqoop

object Archiver {
  def archive[C <: CompressionCodec : ClassManifest](src: String, dest: String): Execution[Long] =
    for {
      config  <- Execution.getConfig
      jobStat <- Execution.fromFuture { ec =>
        val configWithCompress =
          Config(config.toMap ++
            Map(
              "mapred.output.compress" -> "true",
              "mapred.output.compression.type" -> "BLOCK",
              "mapred.output.compression.codec" -> implicitly[ClassManifest[C]].erasure.getName
            )
          )
        implicit val flowDef = new FlowDef
        implicit val localMode = Mode(Args("--hdfs"), new Configuration)
        TypedPipe.from(TextLine(src)).write(CompressibleTypedTsv[String](dest))
        ExecutionContext.newContext(configWithCompress).run
      }
    } yield (ExecutionCounters.fromJobStats(jobStat).get(StatKeys.tuplesWritten).get)
}
