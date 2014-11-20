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

import java.io.InputStream

import scala.io.Source

import scalaz.effect.IO

import org.apache.commons.compress.compressors.bzip2.BZip2CompressorInputStream

import org.apache.hadoop.io.compress.BZip2Codec

import au.com.cba.omnia.thermometer.context.Context
import au.com.cba.omnia.thermometer.core.Thermometer._
import au.com.cba.omnia.thermometer.core.{ThermometerRecordReader, ThermometerSpec}
import au.com.cba.omnia.thermometer.fact.PathFactoids._
import au.com.cba.omnia.thermometer.tools.Streams

object ArchiverSpec extends ThermometerSpec { def is = s2"""

Archiver properties
===================

archives the source to the destination with the provided compression and return the row count $archiveAndReturnCount

"""
  val resourceUrl         = getClass.getResource("/sqoop")
  val data                = Source.fromFile(s"${resourceUrl.getPath}/sales/books/customers/export/new-customers.txt").getLines().toList
  val bzippedRecordReader =
    ThermometerRecordReader[String]((conf, path) => IO {
      val ctx = new Context(conf)
      val in = new BZip2CompressorInputStream(
        ctx.withFileSystem[InputStream](_.open(path))("bzippedRecordReader")
      )
      try Streams.read(in).lines.toList
      finally in.close
    })

  def archiveAndReturnCount = {
    withEnvironment(path(resourceUrl.toString)) {
      executesSuccessfully(Archiver.archive[BZip2Codec](s"$dir/user/sales/books/customers/export", s"$dir/user/sales/books/customers/compressed")) === 3
      facts(s"$dir/user/sales/books/customers/compressed" </> "part-00000.bz2" ==> records(bzippedRecordReader, data))
    }
  }

}
