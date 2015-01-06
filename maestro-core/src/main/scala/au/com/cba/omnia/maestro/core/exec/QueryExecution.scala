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

import org.apache.hadoop.hive.conf.HiveConf.ConfVars

import com.twitter.scalding.Execution

import com.twitter.scrooge.ThriftStruct

import au.com.cba.omnia.ebenezer.scrooge.hive.HiveExecution

import au.com.cba.omnia.maestro.core.hive.HiveTable

/**
  * Configuration for hive queries
  *
  * @param name:     The name of the hive query
  * @param queries:  The hive queries to run
  * @param settings: Hive settings. Empty by default.
  */
@deprecated("Use the query/queries method in Hive instead", "1.15")
case class QueryConfig(
  name: String,
  queries: Seq[String],
  settings: Map[ConfVars, String] = Map.empty
)

/** Factory functions for query configs */
@deprecated("Use the query/queries method in Hive instead", "1.15")
object QueryConfig {
  /** QueryConfig with empty hive settings */
  @deprecated("Use the query/queries method in Hive instead", "1.15")
  def apply(
    name: String,
    queries: String*
  ): QueryConfig =
    QueryConfig(name, queries)

  /** QueryConfig */
  @deprecated("Use the query/queries method in Hive instead", "1.15")
  def apply(
    name: String,
    settings: Map[ConfVars, String],
    queries: String*
  ): QueryConfig =
    QueryConfig(name, queries, settings)
}

/** A trait for API to run hive queries using the execution monad. */
trait QueryExecution {
  /**
    * Runs the specified hive query.
    *
    * When specified, the output value is used to create the target table before the job starts
    */
  @deprecated("Use the query/queries method in Hive instead", "1.15")
  def hiveQuery(config: QueryConfig) : Execution[Unit] =
    HiveExecution.query(config.name, config.settings, config.queries: _*)
 }
