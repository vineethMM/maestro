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

package au.com.cba.omnia.maestro.hive

import scalaz._, Scalaz._

import org.apache.hadoop.fs.Path

import org.apache.hadoop.hive.conf.HiveConf
import org.apache.hadoop.hive.conf.HiveConf.ConfVars.METASTOREWAREHOUSE

import com.twitter.scalding.{Hdfs => _, _}

import com.twitter.scrooge.ThriftStruct

import au.com.cba.omnia.permafrost.hdfs.Hdfs

import au.com.cba.omnia.ebenezer.scrooge.{ParquetScroogeSource, PartitionParquetScroogeSource, PartitionParquetScroogeSink}
import au.com.cba.omnia.beeswax.Hive

import au.com.cba.omnia.maestro.core.partition.Partition
import au.com.cba.omnia.maestro.scalding.ConfHelper
import au.com.cba.omnia.maestro.scalding.ExecutionOps._

/**
  * Base type for partitioned and unpartitioned Hive tables.
  * ST - Type parameter of [[TypedSink]] and [[TypedPipe]]
  * returned in `sink` and `write` methods respectively.
  */
sealed trait GenericHiveTable[T <: ThriftStruct, ST, F] {
  /** Fully qualified SQL reference to table.*/
  val name: String = s"$database.$table"

  /** Path of the table. */
  lazy val tablePath = new Path(externalPath.getOrElse(
    s"${(new HiveConf()).getVar(METASTOREWAREHOUSE)}/$database.db/$table"
  ))

  def database: String
  def table: String
  def externalPath: Option[String]
  def extract: F => ST

  /** Creates a scalding source to read from the hive table.*/
  def source: Mappable[T]
  /** Creates [[Execution]] that writes to `this.sink` using given [[TypedPipe]].*/
  def writeExecution(pipe: TypedPipe[F], append: Boolean = true): Execution[ExecutionCounters] =
    writeToSink( pipe.map(extract), append)

  def writeToSink(pipe: TypedPipe[ST], append: Boolean = true): Execution[ExecutionCounters]
}

sealed trait HiveTable[T <: ThriftStruct, ST] extends GenericHiveTable[T, ST, T]

sealed abstract class PartitionReadWriteSupport[A <: ThriftStruct : Manifest, B : Manifest : TupleSetter] {
  def database: String
  def table: String
  def externalPath: Option[String]
  def tablePath: Path

  def fieldNames: List[String]
  def partitionBatchSize: Int

  /** List of partition column names and type (string by default). */
  val partitionMetadata    = fieldNames.map(n => (n, "string"))
  //Enforce the Hive partition pattern
  val hivePartitionPattern = fieldNames.map(_ + "=%s").mkString("/")
  val partitionGlob        = fieldNames.map(_ => "*").mkString("/")
  val hdfsPartitionGlob    = s"[^_.]$partitionGlob"

  def source: PartitionParquetScroogeSource[B, A] =
    PartitionParquetScroogeSource[B, A](hivePartitionPattern, tablePath.toString)

  def writeToSink(pipe: TypedPipe[(B, A)], append: Boolean = true): Execution[ExecutionCounters] = {
    def modifyConfig(config: Config) = ConfHelper.createUniqueFilenames(config)

    def addPartitions(paths: List[Path]): Execution[Unit] =
      paths.grouped(partitionBatchSize)
        .map((ps: List[Path]) => Execution.hive(Hive.addPartitions(database, table, fieldNames, ps)))
        .toList.sequence_

    def withTable(ops: Path => Execution[ExecutionCounters]): Execution[ExecutionCounters] =
      // Work around: `withSeparateCache` avoids keeping references in the scalding EvalCache, to allow gc
      Execution.withSeparateCache(for {
       _             <- Execution.hive(Hive.createParquetTable[A](database, table, partitionMetadata, externalPath.map(new Path(_))))
       tablePath     <- Execution.hive(Hive.getPath(database, table))
       foldersBefore <- Execution.hdfs(Hdfs.glob(tablePath, hdfsPartitionGlob))
       counter       <- ops(tablePath)
       foldersAfter  <- Execution.hdfs(Hdfs.glob(tablePath, hdfsPartitionGlob))
       created        = foldersAfter.diff(foldersBefore)
       _             <- addPartitions(created)
      } yield counter)

    // Runs the scalding job and gets the counters
    def write(path: Path) =
      pipe
        .writeExecution(PartitionParquetScroogeSink[B, A](hivePartitionPattern, path.toString))
        .getAndResetCounters
        .map(_._2)

    if (append) {
      withTable(write(_).withSubConfig(modifyConfig)) 
    } else {
      /** Steps that happen below :-
        * 1) Create the folder if folder doesn't exist
        * 2) Get list of original parquet files
        * 3) Run the job
        * 4) Get the list of all updated parquet files, including obsolete
        * 5) Get files created by this job
        * 6) Get partitions updated by this job
        * 7) Get obsolete files: existed prior to this job, in partitions which have been updated
        * 8) Delete obsolete files
        */
      withTable { dst =>
        for {
          _            <- Execution.fromHdfs(Hdfs.mkdirs(dst))// Step 1
          oldFiles     <- Execution.fromHdfs(Hdfs.files(dst, "[^_]*", recursive=true))// Step 2
          counters     <- write(dst).withSubConfig(modifyConfig)// Step 3
          allFiles     <- Execution.fromHdfs(Hdfs.files(dst, "[^_]*", recursive=true))// Step 4
          newFiles      = allFiles.filterNot(oldFiles.toSet)// Step 5
          newPartitions = newFiles.map(_.getParent).distinct // Step 6
          toDelete      = oldFiles.filter(f => newPartitions.contains(f.getParent))// Step 7
          _            <- toDelete.map(p => Execution.fromHdfs(Hdfs.delete(p))).sequence//Step 8
        } yield counters
      }
    }
  }
}

/** Information need to address/describe a specific partitioned hive table.*/
case class PartitionedHiveTable[A <: ThriftStruct : Manifest, B : Manifest : TupleSetter](
  database: String, table: String, partition: Partition[A, B], 
  externalPath: Option[String] = None, partitionBatchSize: Int = 100
) extends PartitionReadWriteSupport[A, B] with HiveTable[A, (B, A)] {
  override def fieldNames = partition.fieldNames
  override def extract = (r: A) => (partition.extract(r) -> r)
}

case class GenericPartitionedHiveTable[A <: ThriftStruct : Manifest, B : Manifest : TupleSetter, C](
  database: String, table: String, fieldNames: List[String], extract: C => (B, A),
  externalPath: Option[String] = None, partitionBatchSize: Int = 100
) extends PartitionReadWriteSupport[A, B] with  GenericHiveTable[A, (B, A), C]

/** Information need to address/describe a specific unpartitioned hive table.*/
case class UnpartitionedHiveTable[A <: ThriftStruct : Manifest](
  database: String, table: String, externalPath: Option[String] = None
) extends HiveTable[A, A] {

  override def extract = identity

  override def source =
    ParquetScroogeSource[A](tablePath.toString)

  /** Writes the contents of the pipe to this HiveTable. */
  override def writeToSink(pipe: TypedPipe[A], append: Boolean = true): Execution[ExecutionCounters] = {
    // Creates the hive table
    val setup = Execution.fromHive(
      Hive.createParquetTable[A](database, table, List.empty, externalPath.map(new Path(_))) >>
      Hive.getPath(database, table)
    )

    // Runs the scalding job and gets the counters
    def write(path: Path) = pipe
      .writeExecution(ParquetScroogeSource[A](path.toString))
      .getAndResetCounters
      .map(_._2)

    //If we are appending the files are written to a temporary location and then copied accross
    if (append) {
      def moveFiles(src: Path, dst: Path): Hdfs[Unit] = for {
        _     <- Hdfs.mkdirs(dst) // Create the folder if it doesn't already exist for some reason.
        files <- Hdfs.files(src, "*.parquet")
        uniq   =  UniqueID.getRandom.get
        _     <- files.zipWithIndex.traverse {
                   case (file, idx) => Hdfs.move(file, new Path(dst, f"part-$uniq-$idx%05d.parquet"))
                 }
      } yield ()

      setup.flatMap(dst => 
        Execution.fromHdfs(Hdfs.createTempDir()).bracket(
          tmpDir => Execution.fromHdfs(Hdfs.delete(tmpDir, true))
        )(tmpDir => 
          for {
            counters <- write(tmpDir)
            _        <- Execution.fromHdfs(moveFiles(tmpDir, dst))
          } yield counters
        ))
    } else {
      for {
        dst      <- setup
        counters <- write(dst)
      } yield counters
    }
  }
}

/** Constructors for HiveTable. */
object HiveTable {
  /** Information need to address/describe a specific partitioned hive table.*/
  def apply[A <: ThriftStruct : Manifest, B : Manifest : TupleSetter](
    database: String, table: String, partition: Partition[A, B]
  ): HiveTable[A, (B, A)] =
    PartitionedHiveTable(database, table, partition, None)

  /** Information need to address/describe a specific partitioned hive table.*/
  def apply[A <: ThriftStruct : Manifest, B : Manifest : TupleSetter](
    database: String, table: String, partition: Partition[A, B], path: String
  ): HiveTable[A, (B,A)] =
    PartitionedHiveTable(database, table, partition, Some(path))

  /** Information need to address/describe a specific partitioned hive table.*/
  def apply[A <: ThriftStruct : Manifest, B : Manifest : TupleSetter](
    database: String, table: String, partition: Partition[A, B], 
    pathOpt: Option[String], partitionBatchSize: Int = 100
  ): HiveTable[A, (B, A)] =
    PartitionedHiveTable(database, table, partition, pathOpt, partitionBatchSize)

  /** Information need to address/describe a specific unpartitioned hive table.*/
  def apply[A <: ThriftStruct : Manifest](
    database: String, table: String
  ): HiveTable[A, A] =
    UnpartitionedHiveTable(database, table, None)

  /** Information need to address/describe a specific unpartitioned hive table.*/
  def apply[A <: ThriftStruct : Manifest](
    database: String, table: String, path: String
  ): HiveTable[A, A] =
    new UnpartitionedHiveTable(database, table, Some(path))

  /** Information need to address/describe a specific unpartitioned hive table.*/
  def apply[A <: ThriftStruct : Manifest](
    database: String, table: String, pathOpt: Option[String]
  ): HiveTable[A, A] =
    UnpartitionedHiveTable(database, table, pathOpt)
}
