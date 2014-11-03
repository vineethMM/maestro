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

package au.com.cba.omnia.maestro.core.args

import java.util.UUID

import com.twitter.scalding.{Args, CascadeJob, Job}

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.io.compress.BZip2Codec

import au.com.cba.omnia.thermometer.core.Thermometer._
import au.com.cba.omnia.thermometer.core.{ThermometerSpec, ThermometerSource}
import au.com.cba.omnia.thermometer.fact.PathFactoids

import au.com.cba.omnia.maestro.core.scalding.UniqueJob

import com.cba.omnia.edge.source.compressible.CompressibleTypedPsv

object ModArgsCascadeSpec extends ThermometerSpec { def is = s2"""

ModArgs in Cascade
================

  can use different compression settings in different jobs $individualCompress
"""

  def individualCompress = {
    val cascade = withArgs(Map("dst-root" -> s"$dir/output"))(
      new IndividualCompressionCascadeJob(_)
    )
    cascade.withFacts(
      dir </> "output" </> "uncompressed" </> "part-00000"     ==> PathFactoids.exists,
      dir </> "output" </> "uncompressed" </> "part-00000.bz2" ==> PathFactoids.missing,
      dir </> "output" </> "compressed"   </> "part-00000"     ==> PathFactoids.missing,
      dir </> "output" </> "compressed"   </> "part-00000.bz2" ==> PathFactoids.exists
    )
  }
}

class IndividualCompressionCascadeJob(args: Args) extends CascadeJob(args) {
  val jobs = Seq(
    new CopyJob(ModArgs
      .set("dst", args("dst-root") + "/uncompressed")
      .modify(args)),
    new CopyJob(ModArgs
      .set("dst", args("dst-root") + "/compressed")
      .compressOutput[BZip2Codec]
      .modify(args))
  )
}

class CopyJob(args: Args) extends UniqueJob(args) {
  val dst = args("dst")
  val data = List(
    ("Foo", 1, "AAA"),
    ("Bar", 2, "BBB")
  )

  ThermometerSource(data).write(CompressibleTypedPsv[(String, Int, String)](dst))
}
