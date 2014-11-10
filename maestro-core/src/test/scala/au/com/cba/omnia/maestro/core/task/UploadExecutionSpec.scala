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
    val root      = dir </> "user"
    val destDir   = "hdfs-root" </> "source" </> "normal" </> "mydomain" </> "mytable"
    val expected1 = destDir </> "2014" </> "11" </> "11" </> "mytable_20141111.dat"
    val expected2 = destDir </> "2014" </> "11" </> "12" </> "mytable_20141112.dat"

    val exec = UploadExec.upload(
      "normal", "mydomain", "mytable", "{table}_{yyyyMMdd}.dat",
      s"$root/local-ingest", s"$root/local-archive", "hdfs-root"
    )
    executesSuccessfully(exec).files must containTheSameElementsAs(List(expected1.toString, expected2.toString))
  }

  def custom = withEnvironment(path(getClass.getResource("/upload-execution-custom").toString)) {
    val root      = dir </> "user"
    val destDir   = "hdfs-root" </> "custom-landing"
    val expected1 = destDir </> "2014" </> "11" </> "11" </> "mytable_20141111.dat"
    val expected2 = destDir </> "2014" </> "11" </> "12" </> "mytable_20141112.dat"

    val exec = UploadExec.customUpload(
      "mytable", "{table}_{yyyyMMdd}.dat", s"$root/local-ingest/custom",
      s"$root/local-archive/custom", "hdfs-root/custom-archive", "hdfs-root/custom-landing"
    )
    executesSuccessfully(exec).files must containTheSameElementsAs(List(expected1.toString, expected2.toString))
  }

  def continue = withEnvironment(path(getClass.getResource("/upload-execution-continue").toString)) {
    val root = dir </> "user"

    val exec = for {
      res <- UploadExec.upload(
        "normal", "mydomain", "mytable", "{table}_{yyyyMMdd}.dat",
        s"$root/local-ingest", s"$root/local-archive", "hdfs-root"
      )
      if res.continue
    } yield ()

    execute(exec).get must throwA[Throwable].like { case e => e.getMessage must startWith("Filter failed on") }
  }
}
