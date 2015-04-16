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
        s"r$row-f0", s"r$row-f1", row * 10 + 2.5, row * 10 + 3, Int.MaxValue + row * 10 + 4,
        s"r$row-f5", s"r$row-f6", row * 10 + 7.5, row * 10 + 8, Int.MaxValue + row * 10 + 9
    )).toArray

  def mkStruct20Values(rows: Int): Array[Struct20] =
    (0 until rows).map(row => Struct20(
      s"r$row-f0",  s"r$row-f1",  row * 20 + 2.5,  row * 20 + 3,  Int.MaxValue + row * 20 + 4,
      s"r$row-f5",  s"r$row-f6",  row * 20 + 7.5,  row * 20 + 8,  Int.MaxValue + row * 20 + 9,
      s"r$row-f10", s"r$row-f11", row * 20 + 12.5, row * 20 + 13, Int.MaxValue + row * 20 + 14,
      s"r$row-f15", s"r$row-f16", row * 20 + 17.5, row * 20 + 18, Int.MaxValue + row * 20 + 19
    )).toArray

  def mkStruct30Values(rows: Int): Array[Struct30] =
    (0 until rows).map(row => Struct30(
      s"r$row-f0",  s"r$row-f1",  row * 30 + 2.5,  row * 30 + 3,  Int.MaxValue + row * 30 + 4,
      s"r$row-f5",  s"r$row-f6",  row * 30 + 7.5,  row * 30 + 8,  Int.MaxValue + row * 30 + 9,
      s"r$row-f10", s"r$row-f11", row * 30 + 12.5, row * 30 + 13, Int.MaxValue + row * 30 + 14,
      s"r$row-f15", s"r$row-f16", row * 30 + 17.5, row * 30 + 18, Int.MaxValue + row * 30 + 19,
      s"r$row-f20", s"r$row-f21", row * 30 + 22.5, row * 30 + 23, Int.MaxValue + row * 30 + 24,
      s"r$row-f25", s"r$row-f26", row * 30 + 27.5, row * 30 + 28, Int.MaxValue + row * 30 + 29
    )).toArray

  def mkRow(v: Product): List[String] =
    v.productIterator.map(_.toString).toList

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
