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

package au.com.cba.omnia.maestro.core.misc

import scala.util.matching.Regex

import org.apache.hadoop.conf.Configuration

import au.com.cba.omnia.permafrost.hdfs.Hdfs
import au.com.cba.omnia.permafrost.hdfs.HdfsString._

import au.com.cba.omnia.maestro.core.task.{FromPath, Predetermined, TimeSource}

/**
  * Miscellaneous functions taken from maestro-api. They shouldn't exists here,
  * but should be in properly named traits or objects. See https://github.com/CommBank/maestro/issues/238.
  */
trait Miscellaneous {
  /** Use the current time yyyy-MM-dd as the load time for the data */
  def now(format: String = "yyyy-MM-dd") = {
    val f = new java.text.SimpleDateFormat(format)
    f.setTimeZone(java.util.TimeZone.getTimeZone("UTC"))
    Predetermined(f.format(new java.util.Date))
  }

  /**
    * Derive the load time from the file path using the provided regex.
    * The regex needs to extract the year,month and day into separate capture groups.
    */
  def timeFromPath(regex: Regex): TimeSource = FromPath { (path: String) =>
    val m = regex.pattern.matcher(path)
    m.matches
    s"${m.group(1)}-${m.group(2)}-${m.group(3)}"
  }

  /** Extracts time from path up to hour level using given Regex */
  def timeFromPathHour(regex: Regex): TimeSource = FromPath { (path: String) =>
    val m = regex.pattern.matcher(path)
    m.matches
    s"${m.group(1)}-${m.group(2)}-${m.group(3)}-${m.group(4)}"
  }

  /** Extracts time from path up to seconds level using given Regex */
  def timeFromPathSecond(regex: Regex): TimeSource = FromPath { (path: String) =>
    val m = regex.pattern.matcher(path)
    m.matches
    s"${m.group(1)}-${m.group(2)}-${m.group(3)} ${m.group(4)}:${m.group(5)}:${m.group(6)}"
  }

  /**
    * Creates the _PROCESSED flag to indicate completion of processing in given list of paths
    */
  def createFlagFile(directoryPath : List[String]): Unit={
    directoryPath foreach ((x)=> Hdfs.create(Hdfs.path(s"$x/_PROCESSED")).run(new Configuration))
  }
}
