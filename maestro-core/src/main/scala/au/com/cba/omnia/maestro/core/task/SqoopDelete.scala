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

import org.apache.commons.lang.StringUtils

import au.com.cba.omnia.parlour.SqoopSyntax.ParlourExportDsl
import au.com.cba.omnia.parlour.ParlourExportOptions

/**
  * Handles setup of delete from table before Sqoop export
  *
  * WARNING: This object is not considered part of the maestro API and may be
  * changed without warning.
  */
object SqoopDelete {
  /** Sets DELETE sql query. Throws RuntimeException if sql query already set or table name is not set */
  def trySetDeleteQuery[T <: ParlourExportOptions[T]](options: T): T = {
    if (options.getSqlQuery.fold(false)(StringUtils.isNotEmpty(_))) {
      throw new RuntimeException("SqoopOptions.getSqlQuery must be empty on Sqoop Export with delete from table")
    }

    val tableName: String = options.getTableName.getOrElse(throw new RuntimeException("Cannot create DELETE query before SqoopExport - table name is not set"))
    options.sqlQuery(s"DELETE FROM $tableName")
  }
}
