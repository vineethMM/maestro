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
package split

import scala.collection.JavaConverters._

import com.google.common.base.{Splitter => GoogSplitter}

import com.opencsv.CSVParser

import au.com.cba.omnia.omnitool.Result

/**
  * Represents the notion of splitting a string into parts
  *
  * It should be possible to write a function `List[String] => String`
  * that reverses the split on good input.
  */
case class Splitter(run: String => Result[List[String]])

/** Factory for [[au.com.cba.omnia.maestro.core.split.Splitter]] instances.  */
object Splitter {

  /**
    * Creates a splitter that splits delimited strings.
    *
    * Valid for all input.
    */
  def delimited(delimiter: String) =
    Splitter(DelimitedParseFunction(delimiter))

  private case class DelimitedParseFunction(delimiter: String) extends Function[String, Result[List[String]]] {
    // splitter will not be created until function is used, and so does not have to be serialized
    lazy val splitter = GoogSplitter.on(delimiter)
    def apply(line: String): Result[List[String]] =
      Result.ok(splitter.split(line).asScala.toList)
  }

  /**
    * Creates a splitter that uses a CSV parser, and hence can handle quoted strings.
    *
    * Valid for all input which doesn't end in the middle of a quoted string.
    */
  def csv(delimiter: Char) =
    Splitter(CsvParseFunction(delimiter))

  private case class CsvParseFunction(delimiter: Char) extends Function[String, Result[List[String]]] {
    // parser will not be created until function is used, and so does not have to be serialized
    lazy val parser = new CSVParser(delimiter)
    def apply(line: String): Result[List[String]] =
      Result.safe(parser.parseLine(line).toList)
  }

  /**
    * Creates a splitter that splits according to predefined column lengths.
    *
    * Valid on strings containing exactly the number of characters required to
    * fill all columns.
    */
  def fixed(lengths: List[Int]) = {
    val indicies    = lengths.scanLeft(0)(_ + _)
    val starts      = indicies.init
    val ends        = indicies.tail
    val totalLength = indicies.last
    Splitter(s =>
      if (s.length != totalLength) {
        Result.fail(s"Splitter.fixed expected $totalLength characters in string but got ${s.length} characters")
      }
      else {
        Result.safe((starts, ends).zipped.map(s.substring(_, _)))
      }
    )
  }
}
