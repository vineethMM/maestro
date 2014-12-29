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

package au.com.cba.omnia.maestro.example

import org.apache.hadoop.hive.conf.HiveConf.ConfVars._

import com.twitter.scalding.{Config, Execution}

import au.com.cba.omnia.maestro.api.exec._, Maestro._
import au.com.cba.omnia.maestro.example.thrift.{Account, Customer}

/** Configuration for a customer execution example */
case class CustomerConfig(config: Config) {

  val maestro   = MaestroConfig(
    conf        = config,
    source      = "customer",
    domain      = "customer",
    tablename   = "customer"
  )
  val upload    = maestro.upload()
  val load      = maestro.load[Customer](
    none        = "null"
  )
  val dateTable = maestro.partitionedHiveTable[Customer, (String, String, String)](
    partition   = Partition.byDate(Fields[Customer].EffectiveDate),
    tablename   = "by_date"
  )
  val catTable  = maestro.partitionedHiveTable[Customer, String](
    partition   = Partition.byField(Fields[Customer].Cat),
    tablename   = "by_cat"
  )
  val writeToCatTableQuery = QueryConfig(
    name        = "test",
    settings    = Map(HIVEMERGEMAPFILES -> "true"),
    s"INSERT OVERWRITE TABLE ${catTable.name} PARTITION (partition_cat) SELECT id, name, acct, cat, sub_cat, -10, effective_date, cat AS partition_cat FROM ${dateTable.name}",
    s"SELECT COUNT(*) FROM ${catTable.name}"
  )
}

/** Customer execution example */
object CustomerExecution {

  /** Create an example customer execution */
  def execute: Execution[(LoadSuccess, Long)] = for {
    conf           <- Execution.getConfig.map(CustomerConfig(_))
    uploadInfo     <- upload(conf.upload)
    sources        <- uploadInfo.withSources
    (pipe, ldInfo) <- load[Customer](conf.load, uploadInfo.files)
    loadSuccess    <- ldInfo.withSuccess
    (count1, _)    <- viewHive(conf.dateTable, pipe) zip viewHive(conf.catTable, pipe)
    _              <- hiveQuery(conf.writeToCatTableQuery)
  } yield (loadSuccess, count1)
}
