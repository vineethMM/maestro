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
package validate

import scalaz._, Scalaz._

import org.apache.commons.validator.routines._
import org.apache.commons.validator.GenericValidator

import org.joda.time.DateTime
import org.joda.time.format.{DateTimeFormatter, DateTimeFormat}

import au.com.cba.omnia.maestro.core.data._

/**
  * Validator to check that the record conforms to expectations.
  * 
  * Use this to error out records that don't meet expectations for example
  * checking that records all have a positive balance, etc.
  */
case class Validator[A](run: A => ValidationNel[String, A])

object Validator {
  def all[A](validators: Validator[A]*): Validator[A] =
    Validator(a => validators.toList.traverseU(_.run(a)).as(a))

  def by[A](validation: A => Boolean, message: String): Validator[A] =
    Validator(a => if (validation(a)) a.success else message.failNel)

  def of[A, B](field: Field[A, B], check: Validator[B]): Validator[A] =
    Validator(a => check.run(field.get(a)).as(a))
}

object Check {
  def oneOf(categories: String*): Validator[String] =
    Validator(s => if (categories.contains(s)) s.success else s"""Expected one of [${categories.mkString("|")}] but got ${s}.""".failNel)

  def nonempty: Validator[String] =
    Validator(s => if (!s.isEmpty) s.success else "Value can not be empty.".failNel)

  def isDate(pattern: String): Validator[String]= {
    Validator(s =>
      try {
        val formatter = DateTimeFormat.forPattern(pattern)
        val date: DateTime = formatter.parseDateTime(s)
        s.success
      } catch {
           case e:Exception => s"Date $s is not in the format $pattern".failNel
      }
    )
  }

  def isEmail: Validator[String]=
    Validator(s => if (GenericValidator.isEmail(s)) s.success else s"Data $s not valid email".failNel)

  def isDomain: Validator[String]=
    Validator(s => if (DomainValidator.getInstance().isValid(s)) s.success else s"Data $s not valid Domain".failNel)

  def isIP: Validator[String]=
    Validator(s => if (InetAddressValidator.getInstance().isValid(s)) s.success else s"Data $s not valid IP".failNel)

  def isURL: Validator[String]=
    Validator(s => if (GenericValidator.isUrl(s)) s.success else s"Data $s not valid URL".failNel)
}
