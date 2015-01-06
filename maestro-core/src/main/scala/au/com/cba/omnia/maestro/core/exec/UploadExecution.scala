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

import scala.concurrent.Future
import scala.util.matching.Regex

import scalaz.\&/.{This, That, Both}

import com.twitter.scalding.Execution

import au.com.cba.omnia.omnitool.{Error, Ok, Result}

import au.com.cba.omnia.permafrost.hdfs.Hdfs

import au.com.cba.omnia.maestro.core.scalding.ConfHelper
import au.com.cba.omnia.maestro.core.task.Upload
import au.com.cba.omnia.maestro.core.upload.ControlPattern

/** Information about an upload */
case class UploadInfo(files: List[String]) {
  /** Should we continue the load? */
  def continue: Boolean = !files.isEmpty

  /** Returns a non-emtpy list of sources if we should continue the load.
    * Fails otherwise. */
  def withSources: Execution[List[String]] = Execution.from(
    if (continue) files else throw new Exception("No files to upload")
  )
}

/**
  * Configuration options for upload.
  *
  * Files are copied from the local ingestion path to the three destination paths
  * (HDFS landing path, local archive path, and HDFS archive path). When files
  * are copied to a destination path the effective date of the upload are added
  * to the path. For instance: the actual location of files on HDFS might be
  * `\$hdfsLandingPath/<year>/<month>/<day>/<originalFileName>`.
  *
  * @param localIngestPath:  The local ingestion path where source files are found.
  * @param hdfsLandingPath:  The HDFS landing path where files are copied to.
  * @param localArchivePath: The local archive path where files are archived.
  * @param hdfsArchivePath:  The HDFS archive path, where files are also archived.
  * @param tableName:        The table name or file name in database or project.
  * @param filePattern:      The file pattern, used to identify files to be uploaded.
  *                          Defaults to expecting `<table name><separator><yyyyMMdd>*`.
  * @param controlPattern:   The regex which identifies control files. Defaults to
  *                          detecting files starting with `S_` or files ending with
  *                          `CTR` or `CTL` (case insensitive).
  */
case class UploadConfig(
  localIngestPath: String,
  hdfsLandingPath: String,
  localArchivePath: String,
  hdfsArchivePath: String,
  tablename: String,
  filePattern: String = "{table}?{yyyyMMdd}*",
  controlPattern: Regex = ControlPattern.default
)

/**
  * Push source files to HDFS using [[upload]] and archive them.
  *
  * See the example at `au.com.cba.omnia.maestro.example.CustomerExecution`.
  *
  * In order to run map-reduce jobs, we first need to get our data onto HDFS.
  * [[upload]] copies data files from the local machine onto HDFS and archives
  * the files.
  */
trait UploadExecution {
  /**
    * Pushes source files onto HDFS and archives them locally.
    *
    * `upload` expects data files intended for HDFS to be placed in the local
    * ingestion path. `upload` processes all data files that match a given file
    * pattern. The file pattern format is explained below.
    *
    * Each data file will be copied into the HDFS landing path. Data files are
    * also compressed and archived on the local machine and on HDFS.
    *
    * Some files placed on the local machine are control files. These files
    * are not intended for HDFS and are ignored by `upload`. `upload` will log
    * a message whenever it ignores a control file.
    *
    * When an error occurs, `upload` stops copying files immediately. Once the
    * cause of the error has been addressed, `upload` can be run again to copy
    * any remaining files to HDFS. `upload` will refuse to copy a file if that
    * file or it's control flags are already present in HDFS. If you
    * need to overwrite a file that already exists in HDFS, you will need to
    * delete the file and it's control flags before `upload` will replace it.
    *
    * The file pattern is a string containing the following elements:
    *  - Literals:
    *    - Any character other than `{`, `}`, `*`, `?`, or `\` represents itself.
    *    - `\{`, `\}`, `\*`, `\?`, or `\\` represents `{`, `}`, `*`, `?`, and `\`, respectively.
    *    - The string `{table}` represents the table name.
    *  - Wildcards:
    *    - The character `*` represents an arbitrary number of arbitrary characters.
    *    - The character `?` represents one arbitrary character.
    *  - Date times:
    *    - The string `{<timestamp-pattern>}` represents a JodaTime timestamp pattern.
    *    - `upload` only supports certain date time fields:
    *      - year (y),
    *      - month of year (M),
    *      - day of month (d),
    *      - hour of day (H),
    *      - minute of hour (m), and
    *      - second of minute (s).
    *
    * Some example file patterns:
    *  - `{table}{yyyyMMdd}.DAT`
    *  - `{table}_{yyyyMMdd_HHss}.TXT.*.{yyyyMMddHHss}`
    *  - `??_{table}-{ddMMyy}*`
    *
    * @param config: The upload configuration: [[UploadConfig]].
    *
    * @return The list of copied hdfs files.
    */
  def upload(config: UploadConfig): Execution[UploadInfo] =
    UploadEx.execution(config)
}

/**
  * WARNING: not considered part of the load execution api.
  * We may change this without considering backwards compatibility.
  */
object UploadEx extends Upload {
  def execution(config: UploadConfig): Execution[UploadInfo] = for {
    conf <- Execution.getConfig
    info <- Execution.fromFuture(ec => Future {
      val errorPrefix = s"Failed to upload ${config.tablename} files from ${config.localIngestPath}"
      val res = customUpload(
        config.tablename, config.filePattern, config.localIngestPath,
        config.localArchivePath, config.hdfsArchivePath, config.hdfsLandingPath,
        ConfHelper.getHadoopConf(conf), config.controlPattern
      )
      res match {
        case Ok(copied)        => UploadInfo(copied)
        case Error(Both(e, t)) => throw new Exception(s"$errorPrefix: $e", t)
        case Error(This(e))    => throw new Exception(s"$errorPrefix: $e")
        case Error(That(t))    => throw new Exception(errorPrefix, t)
      }
    }(ec))
  } yield info
}
