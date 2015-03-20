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

import au.com.cba.omnia.maestro.core.data.Field
import au.com.cba.omnia.maestro.core.codec.{Decode, Tag}
import au.com.cba.omnia.maestro.core.thrift.scrooge.StringTriple

trait StringTripleSupport {

  implicit val StringTripleDecode: Decode[StringTriple] = for {
    first  <- Decode.of[String]
    second <- Decode.of[String]
    third  <- Decode.of[String]
  } yield StringTriple(first, second, third)

  implicit val StringTripleTag: Tag[StringTriple] = {
    Tag(_ zip Stream(
      Field("FIRST",  (p: StringTriple) => p.first),
      Field("SECOND", (p: StringTriple) => p.second),
      Field("THIRD",  (p: StringTriple) => p.third)
    ))
  }
}
