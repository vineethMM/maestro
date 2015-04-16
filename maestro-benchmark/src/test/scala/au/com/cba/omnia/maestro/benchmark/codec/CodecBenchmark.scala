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

import au.com.cba.omnia.maestro.benchmark.thrift.{Struct10, Struct20, Struct30}
import au.com.cba.omnia.maestro.core.codec.{Encode, Decode}
import au.com.cba.omnia.maestro.macros.Macros

import org.scalameter.api.{PerformanceTest, Gen}


object Data extends Serializable {
  import Implicits._

  def mkStruct10Values(rows: Int): Array[Struct10] =
    (0 until rows).map(row => Struct10(
        s"r$row-f0", s"r$row-f1", s"r$row-f2", row * 10 + 3, Int.MaxValue + row * 10 + 4,
        s"r$row-f5", s"r$row-f6", s"r$row-f7", row * 10 + 8, Int.MaxValue + row * 10 + 9
    )).toArray

  def mkStruct10Rows(rows: Int): Array[List[String]] =
    mkStruct10Values(rows).map(v => List(
      v._1, v._2, v._3, v._4.toString, v._5.toString,
      v._6, v._7, v._8, v._9.toString, v._10.toString
    ))

  def mkStruct20Values(rows: Int): Array[Struct20] =
    (0 until rows).map(row => Struct20(
      s"r$row-f0",  s"r$row-f1",  s"r$row-f2",  row * 20 + 3,  Int.MaxValue + row * 10 + 4,
      s"r$row-f5",  s"r$row-f6",  s"r$row-f7",  row * 20 + 8,  Int.MaxValue + row * 10 + 9,
      s"r$row-f10", s"r$row-f11", s"r$row-f12", row * 20 + 13, Int.MaxValue + row * 10 + 14,
      s"r$row-f15", s"r$row-f16", s"r$row-f17", row * 20 + 18, Int.MaxValue + row * 10 + 19
    )).toArray

  def mkStruct20Rows(rows: Int): Array[List[String]] =
    mkStruct20Values(rows).map(v => List(
      v._1,  v._2,  v._3,  v._4.toString,  v._5.toString,
      v._6,  v._7,  v._8,  v._9.toString,  v._10.toString,
      v._11, v._12, v._13, v._14.toString, v._15.toString,
      v._16, v._17, v._18, v._19.toString, v._20.toString
    ))

  def mkStruct30Values(rows: Int): Array[Struct30] =
    (0 until rows).map(row => Struct30(
      s"r$row-f0",  s"r$row-f1",  s"r$row-f2",  row * 30 + 3,  Int.MaxValue + row * 10 + 4,
      s"r$row-f5",  s"r$row-f6",  s"r$row-f7",  row * 30 + 8,  Int.MaxValue + row * 10 + 9,
      s"r$row-f10", s"r$row-f11", s"r$row-f12", row * 30 + 13, Int.MaxValue + row * 10 + 14,
      s"r$row-f15", s"r$row-f16", s"r$row-f17", row * 30 + 18, Int.MaxValue + row * 10 + 19,
      s"r$row-f20", s"r$row-f21", s"r$row-f22", row * 30 + 23, Int.MaxValue + row * 10 + 24,
      s"r$row-f25", s"r$row-f26", s"r$row-f27", row * 30 + 28, Int.MaxValue + row * 10 + 29
    )).toArray

  def mkStruct30Rows(rows: Int): Array[List[String]] =
    mkStruct30Values(rows).map(v => List(
      v._1,  v._2,  v._3,  v._4.toString,  v._5.toString,
      v._6,  v._7,  v._8,  v._9.toString,  v._10.toString,
      v._11, v._12, v._13, v._14.toString, v._15.toString,
      v._16, v._17, v._18, v._19.toString, v._20.toString,
      v._21, v._22, v._23, v._24.toString, v._25.toString,
      v._26, v._27, v._28, v._29.toString, v._30.toString
    ))

  val rows           = Gen.range("rows")(10000, 30000, 10000)
  val struct10Values = rows.map(mkStruct10Values(_))
  val struct10Rows   = rows.map(mkStruct10Rows(_))
  val struct20Values = rows.map(mkStruct20Values(_))
  val struct20Rows   = rows.map(mkStruct20Rows(_))
  val struct30Values = rows.map(mkStruct30Values(_))
  val struct30Rows   = rows.map(mkStruct30Rows(_))
}

object CodecBenchmark extends PerformanceTest.OfflineReport {
  import Implicits._

  def testDecode[A : Decode](rows: Gen[Array[List[String]]]) =
    using(rows) in { rows => {
      var i = 0
      while (i < rows.length) {
        Decode.decode[A]("", rows(i))
        i = i+1
      }
    }}

  def testEncode[A : Encode](values: Gen[Array[A]]) =
    using(values) in { values => {
      var i = 0
      while (i < values.length) {
        Encode.encode[A]("", values(i))
        i = i+1
      }
    }}

  performance of "Codecs" in {
    measure method "decode[Struct10]" in {
      testDecode[Struct10](Data.struct10Rows)
    }

    measure method "decode[Struct20]" in {
      testDecode[Struct20](Data.struct20Rows)
    }

    measure method "decode[Struct30]" in {
      testDecode[Struct30](Data.struct30Rows)
    }

    measure method "encode[Struct10]" in {
      testEncode[Struct10](Data.struct10Values)
    }

    measure method "encode[Struct20]" in {
      testEncode[Struct20](Data.struct20Values)
    }

    measure method "encode[Struct30]" in {
      testEncode[Struct30](Data.struct30Values)
    }
  }
}

object Implicits extends Serializable {
  implicit val Struct10Decode: Decode[Struct10] =
    Macros.mkDecode[Struct10]

  implicit val Struct20Decode: Decode[Struct20] =
    Macros.mkDecode[Struct20]

  implicit val Struct30Decode: Decode[Struct30] =
    Macros.mkDecode[Struct30]

  implicit val Struct10Encode: Encode[Struct10] =
    Macros.mkEncode[Struct10]

  implicit val Struct20Encode: Encode[Struct20] =
    Macros.mkEncode[Struct20]

  implicit val Struct30Encode: Encode[Struct30] =
    Macros.mkEncode[Struct30]
}
