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

import com.twitter.scalding.{Args, Job}

import org.apache.hadoop.conf.Configuration

import au.com.cba.omnia.thermometer.core.Thermometer._
import au.com.cba.omnia.thermometer.core.ThermometerSpec
import au.com.cba.omnia.thermometer.fact.PathFactoids

import au.com.cba.omnia.omnitool.{Error, Ok}

object UploadSpec extends ThermometerSpec { def is = s2"""

Upload Cascade
================

  end to end test of upload $endToEnd
"""

  def endToEnd = {
    withEnvironment(path(getClass.getResource("/upload").toString)) {
      val cascade = withArgs(
        Map(
          "hdfs-root"    -> s"$dir/user/hdfs",
          "local-root"   -> s"$dir/user/local",
          "archive-root" -> s"$dir/user/archive")
      )(new UploadTestCascade(_))

      val root = dir </> "user"
      val srcDirs = "account" </> "transaction"
      val dstDirs = "account" </> "transaction" </> "transaction" </> "2014" </> "08" </> "21"
      val file = "transaction_20140821.dat"
      val zippedFile = s"$file.gz"
      cascade.withFacts(
        root </> "local"   </> "dataFeed" </> srcDirs </> file       ==> PathFactoids.missing,
        root </> "archive" </>                dstDirs </> zippedFile ==> PathFactoids.exists,
        root </> "hdfs"    </> "archive"  </> dstDirs </> zippedFile ==> PathFactoids.exists,
        root </> "hdfs"    </> "source"   </> dstDirs </> file       ==> PathFactoids.exists
      )
    }
  }
}

case object UploadLogic extends Upload

class UploadTestCascade(args: Args) extends Job(args) {
  val hdfsRoot    = args("hdfs-root")
  val source      = "account"
  val domain      = "transaction"
  val tablename   = "transaction"
  val localRoot   = args("local-root")
  val archiveRoot = args("archive-root")
  val errors      = s"${hdfsRoot}/errors/${domain}"

  override def validate() {      /* we have no actual scalding jobs to run */ }
  override def clear()    {      /* we have no actual scalding jobs to run */ }
  override def run      = { true /* we have no actual scalding jobs to run */ }

  UploadLogic.upload(source, domain, tablename, "{table}_{yyyyMMdd}*",
    localRoot, archiveRoot, hdfsRoot, new Configuration) match {
    case Ok(_)    => {}
    case Error(e) => throw new Exception(s"Failed to upload file to HDFS: $e")
  }
}
