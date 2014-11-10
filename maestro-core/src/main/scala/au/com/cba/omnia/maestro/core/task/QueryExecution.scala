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

import org.apache.hadoop.hive.conf.HiveConf.ConfVars

import com.twitter.scalding.Execution

import au.com.cba.omnia.ebenezer.scrooge.hive.HiveExecution

import au.com.cba.omnia.maestro.core.hive.HiveTable

/**
  * A trait for API to run hive queries using the execution monad.
  *
  * TODO go over these methods so that they make sense for Execution.
  */
trait QueryExecution {
  /** Runs the specified hive query. */
  def hiveQuery(
    name: String,
    query: String*
  ) : Execution[Unit] =
    HiveExecution.query(name, query: _* )

  /**
    * Runs the specified hive query.
    *
    * The output table is created before running the query.
    */
  def hiveQuery(
    name: String,
    output: HiveTable[_, _],
    query: String*
  ) : Execution[Unit] =
    HiveExecution.query(name, output.sink(), query: _*)

  /** Runs the specified hive query. */
  def hiveQuery(
    name: String,
    hiveSettings: Map[ConfVars, String], query: String*
  ) : Execution[Unit] =
    HiveExecution.query(name, hiveSettings, query: _* )

  /**
    * Runs the specified hive query.
    *
    * The output table is created before running the query.
    */
  def hiveQuery(
    name: String,
    output: HiveTable[_, _],
    hiveSettings: Map[ConfVars, String], query: String*
  ) : Execution[Unit] =
    HiveExecution.query(name, output.sink(), hiveSettings, query: _*)
}

