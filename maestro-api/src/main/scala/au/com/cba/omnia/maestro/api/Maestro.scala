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

package au.com.cba.omnia.maestro.api

import scala.util.matching.Regex

import com.twitter.scalding.{Args, Job, CascadeJob}

import com.twitter.scrooge.ThriftStruct

import org.apache.hadoop.conf.Configuration

import au.com.cba.omnia.permafrost.hdfs.Hdfs
import au.com.cba.omnia.permafrost.hdfs.HdfsString._

import au.com.cba.omnia.maestro.macros.MacroSupport

import au.com.cba.omnia.maestro.core.task._
import au.com.cba.omnia.maestro.core.args.Config

/**
  * Parent class for a more complex maestro job that needs to use cascades. For example, to run hive
  * queries.
  */
abstract class MaestroCascade[A <: ThriftStruct](args: Args) extends CascadeJob(args) with MacroSupport[A] {
  /** Don't run any cascading jobs if the job list is empty. */
  override def run =
    if (jobs.isEmpty) true
    else super.run

  override def validate {
    /* scalding validate class chokes on cascades: don't call super.validate */

    val jobNames = jobs.map(_.name)
    if (jobNames.toSet.size != jobNames.length) {
      throw new Exception(s"Cascade jobs do not have unique names. The names are: ${jobNames.mkString(", ")}")
    }
  }
}

/** Parent class for a simple maestro job that does not need to use cascades or run hive queries.*/
abstract class Maestro[A <: ThriftStruct](args: Args) extends Job(args) with MacroSupport[A]

object Maestro
    extends Load
    with View
    with Query
    with Upload
    with Sqoop
    with Config {
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
  def createFlagFile(directoryPath : List[String]):Unit={
    directoryPath foreach ((x)=> Hdfs.create(Hdfs.path(s"$x/_PROCESSED")).run(new Configuration))
  }
}
