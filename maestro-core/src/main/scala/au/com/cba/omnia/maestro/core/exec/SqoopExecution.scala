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

import java.io.File

import org.apache.log4j.Logger

import scalaz.Monoid

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.io.compress.{GzipCodec, CompressionCodec}

import cascading.flow.FlowDef

import com.twitter.scalding._

import com.cba.omnia.edge.source.compressible.CompressibleTypedTsv

import au.com.cba.omnia.parlour.{SqoopExecution => ParlourExecution, ParlourExportOptions, ParlourImportOptions}

import au.com.cba.omnia.maestro.core.scalding.StatKeys
import au.com.cba.omnia.maestro.core.task.{Sqoop, SqoopDelete}

/**
  * Configuration options for sqoop import
  *
  * Use [[SqoopExecution.createSqoopImportOptions]] to populate the `options` parameter.
  *
  * @param hdfsLandingPath: The HDFS landing directory where files are imported to.
  * @param hdfsArchivePath: The HDFS directory in which to archive files.
  * @param timePath:        The sub-folders representing the effective date, where we place files.
  * @param options:         A DSL for building options to pass to Sqoop
  */
case class SqoopImportConfig[T <: ParlourImportOptions[T]](
  hdfsLandingPath: String,
  hdfsArchivePath: String,
  timePath: String,
  options: T
)

/** Factory functions for [[SqoopImportConfig]] */
object SqoopImportConfig {
  /**
    * Convenience method to populate a parlour import option instance.
    *
    * @param connectionString: Database connection string
    * @param username:         Database username
    * @param password:         Database password
    * @param dbTablename:      Table name in database
    * @param outputFieldsTerminatedBy: Output field terminating character.
    *                          Defaults to '|'
    * @param nullString:       The string to be written for a null value in columns.
    *                          Defaults to the empty string.
    * @param whereCondition:   Where condition. Defaults to no condition.
    * @param initialOptions:   Optional initial parlour options. Not provided by default.
    *
    * @return : Populated parlour option
    */
  def options[T <: ParlourImportOptions[T] : Monoid](
    connectionString: String,
    username: String,
    password: String,
    dbTablename: String,
    outputFieldsTerminatedBy: Char = '|',
    nullString: String             = "",
    whereCondition: Option[String] = None,
    initialOptions: Option[T]      = None
  ): T = SqoopEx.createSqoopImportOptions[T](
    connectionString, username, password, dbTablename, outputFieldsTerminatedBy,
    nullString, whereCondition, initialOptions.getOrElse(implicitly[Monoid[T]].zero)
  )

  /**
   * Convenience method to populate a parlour import option instance.
   * Use it when you want to use SQL Select query to fetch data.
   * If you do not provide `splitBy`, then `numberOfMappers` is set to 1.
   *
   * @param connectionString: database connection string
   * @param username: database username
   * @param password: database password
   * @param query: SQL Select query
   * @param outputFieldsTerminatedBy: output field terminating character
   * @param nullString: The string to be written for a null value in columns
   * @param splitBy: splitting column; if None, then `numberOfMappers` is set to 1 
   * @param options: parlour option to populate
   * 
   * @return : Populated parlour option
   */
  def optionsWithQuery[T <: ParlourImportOptions[T] : Monoid](
    connectionString: String,
    username: String,
    password: String,
    query: String,
    splitBy: Option[String],
    outputFieldsTerminatedBy: Char = '|',
    nullString: String             = "",
    initialOptions: Option[T]      = None
  ): T = SqoopEx.createSqoopImportOptionsWithQuery[T](
    connectionString, username, password, query, splitBy, outputFieldsTerminatedBy,
    nullString, initialOptions.getOrElse(implicitly[Monoid[T]].zero)
  )
}

/**
  * Configuration options for sqoop export
  *
  * Use [[SqoopExecution.createSqoopExportOptions]] to populate the `options` parameter.
  *
  * @param options:         A DSL for building options to pass to Sqoop
  * @param deleteFromTable: If true than delete all data from destination table
  *                         before exporting. Defaults to false.
  */
case class SqoopExportConfig[T <: ParlourExportOptions[T]](
  // the only reason to have this class at all instead of adding
  // deleteFromTable to T is that we want to move processing of deleteFromTable
  // as late as possible so it can detect when adding the option is illegal
  // would be nicer to do sanity checking on T or handle currently illegal Ts
  options: T,
  deleteFromTable: Boolean = false
)

/** Factory functions for [[SqoopExportConfig]] */
object SqoopExportConfig {
  /**
    * Convienience method to populate parlour export options.
    *
    * @param connectionString: Database connection string
    * @param username:         Database username
    * @param password:         Database password
    * @param dbTablename:      Table name in database
    * @param exportDir:        Optional directory to export from, if known in advance.
    *                          Not specified by default.
    * @param inputFieldsTerminatedBy: Output field terminating character.
    *                          Defaults to '|'
    * @param inputNullString:  The string to be written for a null value in columns.
    *                          Defaults to the empty string.
    * @param initialOptions:   Optional initial parlour options. Not provided by default.
    *
    * @return : Populated parlour option
    */
  def options[T <: ParlourExportOptions[T] : Monoid](
    connectionString: String,
    username: String,
    password: String,
    dbTablename: String,
    inputFieldsTerminatedBy: Char = '|',
    inputNullString: String       = "",
    initialOptions: Option[T]     = None
  ): T =
    SqoopEx.createSqoopExportOptions[T](
      connectionString, username, password, dbTablename, inputFieldsTerminatedBy,
      inputNullString, initialOptions
    )
}

/** Methods to import and export from sqoop */
trait SqoopExecution {
  /**
    * Runs a sqoop import from a database table to HDFS.
    *
    * @return Tuple of import directory and number of rows imported
    */
  def sqoopImport[T <: ParlourImportOptions[T]](
    config: SqoopImportConfig[T], condition: String
  ): Execution[(String, Long)] =
    sqoopImport(config.copy(options = config.options.where(condition)))

  /**
    * Runs a sqoop import from a database table to HDFS.
    *
    * @return Tuple of import directory and number of rows imported
    */
  def sqoopImport[T <: ParlourImportOptions[T]](
    config: SqoopImportConfig[T]
  ): Execution[(String, Long)] =
    SqoopEx.importExecution(config)

  /** Exports data from HDFS to a DB via Sqoop. */
  def sqoopExport[T <: ParlourExportOptions[T]](
    config: SqoopExportConfig[T], exportDir: String
  ): Execution[Unit] =
    SqoopEx.exportExecution(config.copy(options = config.options.exportDir(exportDir)))
}

/** Methods to support testing Sqoop execution */
object SqoopExecutionTest {
  /** Set up environment variables so that sqoop knows how to run jobs in testing environment */
  def setupEnv(customMRHome: String = s"${System.getProperty("user.home")}/.ivy2/cache") {
    // very dodgy way of configuring sqoop for testing,
    // but this should only ever have one value for single testing run
    // and the configuration becomes an implementation details hidden from our API
    System.setProperty(SqoopEx.mrHomeKey, customMRHome)
  }
}

/**
  * WARNING: not considered part of the maestro api.
  * We may change this without considering backwards compatibility.
  */
object SqoopEx extends Sqoop {
  // system property key for setting custom hadoop map reduce dir
  // this property is a hack to get testing working without impacting our API
  val mrHomeKey = "MAESTRO_HADOOP_MAPRED_HOME"

  def importExecution[T <: ParlourImportOptions[T]](
    config: SqoopImportConfig[T]
  ): Execution[(String, Long)] = {
    val importPath   = config.hdfsLandingPath + File.separator + config.timePath
    val archivePath  = config.hdfsArchivePath + File.separator + config.timePath
    val logger       = Logger.getLogger("Sqoop")
    val withDestDir  = config.options.targetDir(importPath)
    val withMRHome   = getCustomMRHome.fold(withDestDir)(withDestDir.hadoopMapRedHome(_))
    val sqoopOptions = withMRHome.toSqoopOptions

    logger.info(s"connectionString = ${sqoopOptions.getConnectString}")
    logger.info(s"tableName        = ${sqoopOptions.getTableName}")
    logger.info(s"targetDir        = ${sqoopOptions.getTargetDir}")

    for {
      // can't get count from sqoop, but can get it from archive job
      _     <- ParlourExecution.sqoopImport(withMRHome)
      count <- archive[GzipCodec](importPath, archivePath)
    } yield (importPath, count)
  }

  def exportExecution[T <: ParlourExportOptions[T]](
    config: SqoopExportConfig[T]
  ): Execution[Unit] = {
    val withDelete =
      if (config.deleteFromTable) SqoopDelete.trySetDeleteQuery(config.options)
      else config.options
    val withMRHome = getCustomMRHome.fold(withDelete)(withDelete.hadoopMapRedHome(_))
    ParlourExecution.sqoopExport(withMRHome)
  }

  // mirrors createSqoopImportOptions from Sqoop
  // except initialOptions is Option[T], to avoid duplicating that code above
  def createSqoopExportOptions[T <: ParlourExportOptions[T] : Monoid](
    connectionString: String,
    username: String,
    password: String,
    dbTablename: String,
    inputFieldsTerminatedBy: Char,
    inputNullString: String,
    initialOptions: Option[T]
  ): T = {
    val initial = initialOptions.getOrElse(implicitly[Monoid[T]].zero)
    initial
      .connectionString(connectionString)
      .username(username)
      .password(password)
      .tableName(dbTablename)
      .inputFieldsTerminatedBy(inputFieldsTerminatedBy)
      .inputNull(inputNullString)
  }

  def archive[C <: CompressionCodec : ClassManifest](
    src: String, dest: String
  ): Execution[Long] =
    Execution.getConfigMode.flatMap { case (config, mode) =>
      Execution.fromFuture { cec =>
        val configWithCompress = Config(config.toMap ++ Map(
          "mapred.output.compress"          -> "true",
          "mapred.output.compression.type"  -> "BLOCK",
          "mapred.output.compression.codec" -> implicitly[ClassManifest[C]].erasure.getName
        ))
        TypedPipe.from(TextLine(src))
          .writeExecution(CompressibleTypedTsv[String](dest))
          .getAndResetCounters
          .map( _._2.get(StatKeys.tuplesWritten).getOrElse(0L) )  
          .run(configWithCompress, mode)(cec)
      }
    }

  def getCustomMRHome: Option[String] =
    Option(System.getProperty(mrHomeKey))
}
