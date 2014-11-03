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

package au.com.cba.omnia.maestro.core
package task

import com.twitter.scalding.{Args, Job, TypedPipe, TypedTsv}

import org.apache.hadoop.fs.Path

import org.specs2.execute.{Failure, FailureException, Result}

import au.com.cba.omnia.thermometer.core.{ThermometerSpec, ThermometerSource}
import au.com.cba.omnia.thermometer.context.Context
import au.com.cba.omnia.thermometer.tools.Jobs

import au.com.cba.omnia.maestro.core.clean.Clean
import au.com.cba.omnia.maestro.core.codec.{Decode, Tag}
import au.com.cba.omnia.maestro.core.filter.RowFilter
import au.com.cba.omnia.maestro.core.split.Splitter
import au.com.cba.omnia.maestro.core.validate.Validator

import au.com.cba.omnia.maestro.core.thrift.scrooge.StringPair

object LoadSpec extends ThermometerSpec with LoadTestUtil { def is = s2"""

Load properties
===============

loadProcess applies delimiter based splitter correctly $loadProcessSplitsDelim
loadProcess applies fixed length splitter correctly    $loadProcessSplitsFixed

"""

  def loadProcessSplitsDelim = {
    val input      = List("col1|col02", "colA|col_B", "colI|colII")
    val splitter   = Splitter.delimited("|")
    val test       = (pair:StringPair) => pair.first.length == 4 &&
    pair.second.length == 5

    loadProcessStringPairTest(input, splitter, clean, validator, filter, test)
  }

  def loadProcessSplitsFixed = {
    val input      = List("col1col02", "colAcol_B", "colIcolII")
    val splitter   = Splitter.fixed(List(4,5))
    val test       = (pair:StringPair) => pair.first.length == 4 &&
                                          pair.second.length == 5

    loadProcessStringPairTest(input, splitter, clean, validator, filter, test)
  }

  def loadProcessStringPairTest(input: List[String], splitter: Splitter,
    clean: Clean, validator: Validator[StringPair], filter: RowFilter,
    test: StringPair => Boolean): Result = {

    val outFile = "output"
    val errFile = "errors"
    val outPath = new Path(outFile)
    val errPath = new Path(errFile)

    val testJob = new LoadProcessStringPairTestJob(scaldingArgs, input,
      splitter, clean, validator, filter, outFile, errFile, test)
    val context = Context(jobConf)

    val result  = Jobs.runJob(testJob)

    result match {
      case Some(err) => {
        err.fold(println(_), _.printStackTrace())
        failure(err.fold(m => m, _.toString()))
      }
      case None => {
        if (context.exists(errPath)) failure("errors occured during loadProcess")
        else if (!context.exists(outPath)) failure("no output")
        else success
      }
    }
  }
}

class LoadProcessStringPairTestJob
  (args: Args, input: List[String], splitter: Splitter, clean: Clean,
    validator: Validator[StringPair], filter: RowFilter, outFile: String,
    errFile: String, test: StringPair => Boolean
  )(implicit decode: Decode[StringPair], tag: Tag[StringPair]) extends Job(args) {
  val in = ThermometerSource(input).map(l => RawRow(l, Nil))

  Load.loadProcess[StringPair](in, splitter, errFile, clean, validator, filter, "\0")
    .map(pair =>
      if (!test(pair)) throw new FailureException(Failure(s"bad result: $pair"))
      else (pair.first, pair.second))
    .write(TypedTsv[(String,String)](outFile))
}
