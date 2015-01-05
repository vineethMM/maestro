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

import au.com.cba.omnia.thermometer.core.ThermometerSpec
import au.com.cba.omnia.thermometer.core.Thermometer._
import au.com.cba.omnia.thermometer.fact.PathFactoids._

private object UploadExec extends UploadExecution

object UploadExecutionSpec extends ThermometerSpec { def is = s2"""

Upload execution properties
=========================

  can upload using execution monad                        $normal
  can custom upload using execution monad                 $custom
  can use continue to stop after an empty upload          $continue
"""

  def normal = withEnvironment(path(getClass.getResource("/upload-execution-normal").toString)) {
    val root      = s"$dir/user"
    val dirStruct = "normal/mydomain/mytable"
    val destDir   = "hdfs-root/source/$dirStruct"
    val conf      = UploadConfig(
      s"$root/local-ingest/dataFeed/normal/mydomain", destDir,
      s"$root/local-archive/$dirStruct", "hdfs-root/archive/$dirStruct", "mytable"
    )

    executesSuccessfully(UploadExec.upload(conf)).files must containTheSameElementsAs(List(
      s"$destDir/2014/11/11/mytable_20141111.dat",
      s"$destDir/2014/11/12/mytable_20141112.dat"
    ))
  }

  def custom = withEnvironment(path(getClass.getResource("/upload-execution-custom").toString)) {
    val root      = s"$dir/user"
    val destDir   = "hdfs-root/custom-landing"
    val conf      = UploadConfig(
      s"$root/local-ingest/custom", "hdfs-root/custom-landing", s"$root/local-archive/custom",
      "hdfs-root/custom-archive", "mytable", "{table}_{yyyyMMdd}.dat"
    )

    executesSuccessfully(UploadExec.upload(conf)).files must containTheSameElementsAs(List(
      s"$destDir/2014/11/11/mytable_20141111.dat",
      s"$destDir/2014/11/12/mytable_20141112.dat"
    ))
  }

  def continue = withEnvironment(path(getClass.getResource("/upload-execution-continue").toString)) {
    val root      = s"$dir/user"
    val dirStruct = "normal/mydomain/mytable"
    val conf      = UploadConfig(
      s"$root/local-ingest/dataFeed/normal/mydomain", "hdfs-root/source/$dirStruct",
      s"$root/local-archive/$dirStruct", "hdfs-root/archive/$dirStruct", "mytable"
    )
    val exec = for {
      res <- UploadExec.upload(conf)
      if res.continue
    } yield ()

    execute(exec).get must throwA[Throwable].like { case e => e.getMessage must startWith("Filter failed on") }
  }
}
