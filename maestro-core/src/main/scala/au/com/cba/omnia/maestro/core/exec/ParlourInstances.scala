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

package au.com.cba.omnia.maestro.core.exec

import scalaz._, Scalaz._

import au.com.cba.omnia.parlour.SqoopSyntax.{ParlourImportDsl, ParlourExportDsl, TeradataParlourImportDsl, TeradataParlourExportDsl}

/**
  * Type class instances for parlour configuration dsls.
  *
  * Monoid instances are required so we can instantiate empty values as defaults.
  *
  * TODO move to parlour
  */
object ParlourInstances {
  implicit val parlourImportDslMonoid: Monoid[ParlourImportDsl] =
    new Monoid[ParlourImportDsl] {
      def zero = ParlourImportDsl()
      def append(x: ParlourImportDsl, y: => ParlourImportDsl) = ParlourImportDsl(y.updates ++ x.updates)
    }

  implicit val parlourExportDslMonoid: Monoid[ParlourExportDsl] =
    new Monoid[ParlourExportDsl] {
      def zero = ParlourExportDsl()
      def append(x: ParlourExportDsl, y: => ParlourExportDsl) = ParlourExportDsl(y.updates ++ x.updates)
    }

  implicit val teradataParlourImportDslMonoid: Monoid[TeradataParlourImportDsl] =
    new Monoid[TeradataParlourImportDsl] {
      def zero = TeradataParlourImportDsl()
      def append(x: TeradataParlourImportDsl, y: => TeradataParlourImportDsl) = TeradataParlourImportDsl(y.updates ++ x.updates)
    }

  implicit val teradataParlourExportDslMonoid: Monoid[TeradataParlourExportDsl] =
    new Monoid[TeradataParlourExportDsl] {
      def zero = TeradataParlourExportDsl()
      def append(x: TeradataParlourExportDsl, y: => TeradataParlourExportDsl) = TeradataParlourExportDsl(y.updates ++ x.updates)
    }
}
