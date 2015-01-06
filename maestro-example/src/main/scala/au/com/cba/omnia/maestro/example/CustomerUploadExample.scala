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

package au.com.cba.omnia.maestro.example

import com.twitter.scalding.Args

import scalaz._, Scalaz._

import au.com.cba.omnia.maestro.api.Maestro
import au.com.cba.omnia.maestro.example.thrift.Customer

import au.com.cba.omnia.omnitool.{Error, Ok, Result}

class CustomerUploadExample(args: Args) extends Maestro[Customer](args) {
  val hdfsRoot    = args("hdfs-root")
  val source      = "customer"
  val domain      = "customer"
  val filePattern = "{table}_{yyyyMMdd_HHmm}*"
  val localRoot   = args("local-root")
  val archiveRoot = args("archive-root")
  val conf        = Maestro.configuration(args)

  val byDateResult = Maestro.upload(source, domain, "by_date", filePattern, localRoot, archiveRoot, hdfsRoot, conf)
  val byIdResult   = Maestro.upload(source, domain, "by_id",   filePattern, localRoot, archiveRoot, hdfsRoot, conf)

  List(byDateResult, byIdResult).sequence_ match {
    case Error(_) => {
      // do not post-process files when there was an error copying them
      // report the error here
      ()
    }
    case Ok(()) => {
      // continue with post-processing here
      ()
    }
  }
}
