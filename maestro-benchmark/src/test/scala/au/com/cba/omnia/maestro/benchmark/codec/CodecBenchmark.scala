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

package au.com.cba.omnia.maestro.benchmark.codec

import java.io.Serializable

import au.com.cba.omnia.maestro.benchmark.thrift.Basic
import au.com.cba.omnia.maestro.core.codec.{Encode, Decode}
import au.com.cba.omnia.maestro.macros.Macros

import org.scalameter.api.{PerformanceTest, Gen}


object Data extends Serializable {
  implicit val BasicDecode: Decode[Basic] =
    Macros.mkDecode[Basic]

  implicit val BasicEncode: Encode[Basic] =
    Macros.mkEncode[Basic]

  def data(n: Int): Array[Basic] = (1 to n).map(n =>
    Basic("basic-" + n, n, Int.MaxValue.toLong + n.toLong)
  ).toArray

  def source(n: Int): Array[List[String]] =
    data(n).map(d => Encode.encode("", d))
}

object CodecBenchmark extends PerformanceTest.OfflineRegressionReport {
  import Data._

  val sizes = Gen.range("size")(4000, 20000, 4000)

  val toEncode = sizes.map(data(_))

  val toDecode = sizes.map(source(_))

  performance of "Codecs" in {
    measure method "decode" in {
      using(toDecode) in { d =>
        d.map(r => Decode.decode[Basic]("", r))
      }
    }

    measure method "encode" in {
      using(toEncode) in { e =>
        e.map(b => Encode.encode[Basic]("", b))
      }
    }
  }
}
