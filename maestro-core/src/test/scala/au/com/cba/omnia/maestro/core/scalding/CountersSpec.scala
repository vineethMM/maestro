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

package au.com.cba.omnia.maestro.core.scalding

import cascading.cascade.Cascade

import com.twitter.scalding.{Args, CascadeJob, Job, TypedPipe, TypedPsv}

import org.apache.hadoop.conf.Configuration

import au.com.cba.omnia.thermometer.core.{ThermometerSpec, ThermometerSource}

object CountersSpec extends ThermometerSpec { def is = s2"""
CountersSpec
============

counters should:
  give correct stats for each job: $correctJobStats
"""

  def correctJobStats = {
    val cascade = withArgs(Map("dir" -> dir))(
      new CorrectJobStatsCascadeJob(_, counters => {
        counters.map(_.tuplesRead)    must_== List(25, 10, 10, 5)
        counters.map(_.tuplesWritten) must_== List(20, 10,  5, 5)
      })
    )
    cascade.runsOk
  }
}

class CorrectJobStatsCascadeJob(
  args: Args,
  checkStats: List[Counters] => Unit
) extends CascadeJob(args) {
  val dir = args("dir")

  val copyJob = new NamedJob("copy", args) {
    ThermometerSource(1 to 10).write(TypedPsv[Int](s"$dir/copied"))
  }
  val filterJob = new NamedJob("filter", args) {
    TypedPipe.from(TypedPsv[Int](s"$dir/copied")).filter(_ > 5).write(TypedPsv[Int](s"$dir/filtered"))
  }
  val copyJob2 = new NamedJob("increment", args) {
    TypedPipe.from(TypedPsv[Int](s"$dir/filtered")).write(TypedPsv[Int](s"$dir/copied2"))
  }

  val jobs = Seq(copyJob, filterJob, copyJob2)

  override def postProcessCascade(cascade: Cascade) {
    checkStats(List(
      Counters.cascade(cascade),
      Counters.job(cascade, copyJob),
      Counters.job(cascade, filterJob),
      Counters.job(cascade, copyJob2)
    ))
  }

  override def validate {}
}
