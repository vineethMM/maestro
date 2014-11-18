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
package data

import com.twitter.scrooge.ThriftStruct
import org.scalacheck.Gen
import org.scalacheck.Prop.forAll
object FieldSpec extends test.Spec { def is = s2"""

Field equality
===========

Fields with
  different names should NOT be equal and should have different hashCode  $fieldNameNotEqual
  equal names and column type should be equal and have equal hashCode     $fieldNameAndTypeEqual


"""
  val genDifferentString = for {
    x <- Gen.alphaStr
    y <- Gen.alphaStr
    if (x != y)
  } yield (x ,y)

  def fieldNameNotEqual = forAll(genDifferentString) {
    case (name, name2) => {
      val fun = (x: ThriftStruct) => ""
      val res = Field(name, fun)
      val res2 = Field(name2, fun)
      ("no equal" |: res != res2 && res.hashCode != res2.hashCode )
    }
  }

  def fieldNameAndTypeEqual = forAll { (name: String, f: String => Int, f2: String => Double) => {
    val res = Field(name, f)
    val res2 = Field(name, f2)
    ("equal" |: res == res2 && res.hashCode == res2.hashCode )

  }

  }
 
}
