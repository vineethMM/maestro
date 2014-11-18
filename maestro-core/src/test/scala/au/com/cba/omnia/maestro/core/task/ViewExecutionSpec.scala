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

import au.com.cba.omnia.thermometer.core.{Thermometer, ThermometerSource, ThermometerSpec}, Thermometer._
import au.com.cba.omnia.thermometer.fact.PathFactoid
import au.com.cba.omnia.thermometer.fact.PathFactoids._
import au.com.cba.omnia.thermometer.hive.HiveSupport

import au.com.cba.omnia.maestro.core.data.Field
import au.com.cba.omnia.maestro.core.hive.HiveTable
import au.com.cba.omnia.maestro.core.partition.Partition

import au.com.cba.omnia.maestro.core.thrift.scrooge.StringPair

private object ViewExec extends ViewExecution

object ViewExecutionSpec extends ThermometerSpec with HiveSupport { def is = s2"""

View execution properties
=========================

  can view using execution monad                          $normal
  view executions can be composed with zip                $zipped

  can view in a hive table using execution monad          $normalHive
  view hive executions can be composed with flatMap       $flatMappedHive
  view hive executions can be composed with zip           $zippedHive
"""

  def normal = {
    val exec = ViewExec.view(byFirst, s"$dir/normal")(source)
    executesSuccessfully(exec) must_== 4
    facts(
      dir </> "normal" </> "A" </> "part-*.parquet" ==> matchesFile,
      dir </> "normal" </> "B" </> "part-*.parquet" ==> matchesFile
    )
  }

  def zipped = {
    val exec = ViewExec.view(byFirst, s"$dir/zipped/by_first")(source)
      .zip(ViewExec.view(bySecond, s"$dir/zipped/by_second")(source))
    executesSuccessfully(exec) must_== ((4, 4))
    facts(
      dir </> "zipped" </> "by_first"  </> "A" </> "part-*.parquet" ==> matchesFile,
      dir </> "zipped" </> "by_first"  </> "B" </> "part-*.parquet" ==> matchesFile,
      dir </> "zipped" </> "by_second" </> "1" </> "part-*.parquet" ==> matchesFile,
      dir </> "zipped" </> "by_second" </> "2" </> "part-*.parquet" ==> matchesFile
    )
  }

  def normalHive = {
    val exec = ViewExec.viewHive(tableByFirst("normalHive"))(source)
    executesSuccessfully(exec) must_== 4
    facts(
      hiveWarehouse </> "normalhive.db" </> "by_first" </> "partition_first=A" </> "part-*.parquet" ==> matchesFile,
      hiveWarehouse </> "normalhive.db" </> "by_first" </> "partition_first=B" </> "part-*.parquet" ==> matchesFile
    )
  }

  def flatMappedHive = {
    val exec = for {
      count1 <- ViewExec.viewHive(tableByFirst("flatMappedHive"))(source)
      count2 <- ViewExec.viewHive(tableBySecond("flatMappedHive"))(source)
    } yield (count1, count2)
    executesSuccessfully(exec) must_== ((4, 4))
    facts(
      hiveWarehouse </> "flatmappedhive.db" </> "by_first"  </> "partition_first=A"  </> "part-*.parquet" ==> matchesFile,
      hiveWarehouse </> "flatmappedhive.db" </> "by_first"  </> "partition_first=B"  </> "part-*.parquet" ==> matchesFile,
      hiveWarehouse </> "flatmappedhive.db" </> "by_second" </> "partition_second=1" </> "part-*.parquet" ==> matchesFile,
      hiveWarehouse </> "flatmappedhive.db" </> "by_second" </> "partition_second=2" </> "part-*.parquet" ==> matchesFile
    )
  }

  def zippedHive = {
    val exec = ViewExec.viewHive(tableByFirst("zippedHive"))(source)
      .zip(ViewExec.viewHive(tableBySecond("zippedHive"))(source))
    executesSuccessfully(exec) must_== ((4, 4))
    facts(
      hiveWarehouse </> "zippedhive.db" </> "by_first"  </> "partition_first=A"  </> "part-*.parquet" ==> matchesFile,
      hiveWarehouse </> "zippedhive.db" </> "by_first"  </> "partition_first=B"  </> "part-*.parquet" ==> matchesFile,
      hiveWarehouse </> "zippedhive.db" </> "by_second" </> "partition_second=1" </> "part-*.parquet" ==> matchesFile,
      hiveWarehouse </> "zippedhive.db" </> "by_second" </> "partition_second=2" </> "part-*.parquet" ==> matchesFile
    )
  }


  def tableByFirst(database: String) =
    HiveTable(database, "by_first", byFirst, None)

  def tableBySecond(database: String) =
    HiveTable(database, "by_second", bySecond, None)

  def byFirst  = Partition.byField(Field[StringPair, String]("first", _.first))
  def bySecond = Partition.byField(Field[StringPair, String]("second", _.second))

  def source = ThermometerSource(data)
  def data = List(
    StringPair("A", "1"),
    StringPair("A", "2"),
    StringPair("B", "1"),
    StringPair("B", "2")
  )

  def matchesFile = PathFactoid((context, path) => !context.glob(path).isEmpty)
}
