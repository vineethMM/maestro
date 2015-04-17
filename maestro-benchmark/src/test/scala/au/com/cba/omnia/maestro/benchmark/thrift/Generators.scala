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

package au.com.cba.omnia.maestro.benchmark.thrift

import org.scalameter.api.Gen

/** Contains generators for thrift types */
object Generators extends Serializable {
  def mkStruct10Values(rows: Int): Array[Struct10] =
    (0 until rows).map(row => Struct10(
        s"r$row-f0", s"r$row-f1", s"r$row-f2", row * 10 + 3, Int.MaxValue + row * 10 + 4,
        s"r$row-f5", s"r$row-f6", s"r$row-f7", row * 10 + 8, Int.MaxValue + row * 10 + 9
    )).toArray

  def mkStruct20Values(rows: Int): Array[Struct20] =
    (0 until rows).map(row => Struct20(
      s"r$row-f0",  s"r$row-f1",  s"r$row-f2",  row * 20 + 3,  Int.MaxValue + row * 10 + 4,
      s"r$row-f5",  s"r$row-f6",  s"r$row-f7",  row * 20 + 8,  Int.MaxValue + row * 10 + 9,
      s"r$row-f10", s"r$row-f11", s"r$row-f12", row * 20 + 13, Int.MaxValue + row * 10 + 14,
      s"r$row-f15", s"r$row-f16", s"r$row-f17", row * 20 + 18, Int.MaxValue + row * 10 + 19
    )).toArray

  def mkStruct30Values(rows: Int): Array[Struct30] =
    (0 until rows).map(row => Struct30(
      s"r$row-f0",  s"r$row-f1",  s"r$row-f2",  row * 30 + 3,  Int.MaxValue + row * 10 + 4,
      s"r$row-f5",  s"r$row-f6",  s"r$row-f7",  row * 30 + 8,  Int.MaxValue + row * 10 + 9,
      s"r$row-f10", s"r$row-f11", s"r$row-f12", row * 30 + 13, Int.MaxValue + row * 10 + 14,
      s"r$row-f15", s"r$row-f16", s"r$row-f17", row * 30 + 18, Int.MaxValue + row * 10 + 19,
      s"r$row-f20", s"r$row-f21", s"r$row-f22", row * 30 + 23, Int.MaxValue + row * 10 + 24,
      s"r$row-f25", s"r$row-f26", s"r$row-f27", row * 30 + 28, Int.MaxValue + row * 10 + 29
    )).toArray

  def mkRow(v: Struct10): List[String] = List(
    v._1, v._2, v._3, v._4.toString, v._5.toString,
    v._6, v._7, v._8, v._9.toString, v._10.toString
  )

  def mkRow(v: Struct20): List[String] = List(
    v._1,  v._2,  v._3,  v._4.toString,  v._5.toString,
    v._6,  v._7,  v._8,  v._9.toString,  v._10.toString,
    v._11, v._12, v._13, v._14.toString, v._15.toString,
    v._16, v._17, v._18, v._19.toString, v._20.toString
  )

  def mkRow(v: Struct30): List[String] = List(
    v._1,  v._2,  v._3,  v._4.toString,  v._5.toString,
    v._6,  v._7,  v._8,  v._9.toString,  v._10.toString,
    v._11, v._12, v._13, v._14.toString, v._15.toString,
    v._16, v._17, v._18, v._19.toString, v._20.toString,
    v._21, v._22, v._23, v._24.toString, v._25.toString,
    v._26, v._27, v._28, v._29.toString, v._30.toString
  )

  /** Generator for the number of rows we benchmark */
  val rows: Gen[Int] =
    Gen.range("rows")(10000, 30000, 5000)

  /** Generator for [[Struct10]]s */
  val struct10Values: Gen[Array[Struct10]] =
    rows.map(mkStruct10Values(_))

  /** Generator for [[Struct20]]s */
  val struct20Values: Gen[Array[Struct20]] =
    rows.map(mkStruct20Values(_))

  /** Generator for [[Struct30]]s */
  val struct30Values: Gen[Array[Struct30]] =
    rows.map(mkStruct30Values(_))

  /** Generator for rows which can be parsed into [[Struct10]]s */
  val struct10Rows: Gen[Array[List[String]]] =
    struct10Values.map(_.map(mkRow))

  /** Generator for rows which can be parsed into [[Struct20]]s */
  val struct20Rows: Gen[Array[List[String]]] =
    struct20Values.map(_.map(mkRow))

  /** Generator for rows which can be parsed into [[Struct30]]s */
  val struct30Rows: Gen[Array[List[String]]] =
    struct30Values.map(_.map(mkRow))
}
