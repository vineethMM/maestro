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

package au.com.cba.omnia.maestro.api

import com.twitter.scrooge.ThriftStruct

package object exec {
  type MacroSupport[A <: ThriftStruct] = au.com.cba.omnia.maestro.macros.MacroSupport[A]

  type JobStatus       = au.com.cba.omnia.maestro.core.exec.JobStatus
  type JobFailure      = au.com.cba.omnia.maestro.core.exec.JobFailure
  val  JobFinished     = au.com.cba.omnia.maestro.core.exec.JobFinished
  val  JobNotReady     = au.com.cba.omnia.maestro.core.exec.JobNotReady
  val  JobNeverReady   = au.com.cba.omnia.maestro.core.exec.JobNeverReady

  type HiveTable[A <: ThriftStruct, B] = au.com.cba.omnia.maestro.core.hive.HiveTable[A, B]
  type Partition[A, B] = au.com.cba.omnia.maestro.core.partition.Partition[A, B]
  val  HiveTable       = au.com.cba.omnia.maestro.core.hive.HiveTable
  val  Partition       = au.com.cba.omnia.maestro.core.partition.Partition
  val  HivePartition   = au.com.cba.omnia.maestro.core.partition.HivePartition

  type TimeSource      = au.com.cba.omnia.maestro.core.time.TimeSource
  val  TimeSource      = au.com.cba.omnia.maestro.core.time.TimeSource

  type Clean           = au.com.cba.omnia.maestro.core.clean.Clean
  val  Clean           = au.com.cba.omnia.maestro.core.clean.Clean

  type Validator[A]    = au.com.cba.omnia.maestro.core.validate.Validator[A]
  val  Validator       = au.com.cba.omnia.maestro.core.validate.Validator
  val  Check           = au.com.cba.omnia.maestro.core.validate.Check

  type RowFilter       = au.com.cba.omnia.maestro.core.filter.RowFilter
  val  RowFilter       = au.com.cba.omnia.maestro.core.filter.RowFilter

  type Splitter        = au.com.cba.omnia.maestro.core.split.Splitter
  val  Splitter        = au.com.cba.omnia.maestro.core.split.Splitter

  type Decode[A]       = au.com.cba.omnia.maestro.core.codec.Decode[A]
  type Transform[A, B] = au.com.cba.omnia.maestro.core.transform.Transform[A, B]
  type Tag[A]          = au.com.cba.omnia.maestro.core.codec.Tag[A]
  type Field[A, B]     = au.com.cba.omnia.maestro.core.data.Field[A, B]
  val  Macros          = au.com.cba.omnia.maestro.macros.Macros

  type GuardFilter     = au.com.cba.omnia.maestro.core.hdfs.GuardFilter
  val  Guard           = au.com.cba.omnia.maestro.core.hdfs.Guard

  type Config          = com.twitter.scalding.Config
  type Execution[A]    = com.twitter.scalding.Execution[A]
  val  Execution       = com.twitter.scalding.Execution

  type MaestroConfig   = au.com.cba.omnia.maestro.core.exec.MaestroConfig
  val  MaestroConfig   = au.com.cba.omnia.maestro.core.exec.MaestroConfig

  type UploadConfig    = au.com.cba.omnia.maestro.core.exec.UploadConfig
  type UploadInfo      = au.com.cba.omnia.maestro.core.exec.UploadInfo
  val  UploadConfig    = au.com.cba.omnia.maestro.core.exec.UploadConfig
  val  UploadInfo      = au.com.cba.omnia.maestro.core.exec.UploadInfo

  type LoadConfig[A]   = au.com.cba.omnia.maestro.core.exec.LoadConfig[A]
  type LoadInfo        = au.com.cba.omnia.maestro.core.exec.LoadInfo
  type LoadSuccess     = au.com.cba.omnia.maestro.core.exec.LoadSuccess
  type LoadFailure     = au.com.cba.omnia.maestro.core.exec.LoadFailure
  val  LoadConfig      = au.com.cba.omnia.maestro.core.exec.LoadConfig
  val  LoadSuccess     = au.com.cba.omnia.maestro.core.exec.LoadSuccess
  val  LoadFailure     = au.com.cba.omnia.maestro.core.exec.LoadFailure
  val  EmptyLoad       = au.com.cba.omnia.maestro.core.exec.EmptyLoad

  type ViewConfig[A <: ThriftStruct, B] = au.com.cba.omnia.maestro.core.exec.ViewConfig[A, B]
  val  ViewConfig      = au.com.cba.omnia.maestro.core.exec.ViewConfig

  type QueryConfig     = au.com.cba.omnia.maestro.core.exec.QueryConfig
  val  QueryConfig     = au.com.cba.omnia.maestro.core.exec.QueryConfig

  type ParlourImportDsl         = au.com.cba.omnia.parlour.SqoopSyntax.ParlourImportDsl
  type ParlourExportDsl         = au.com.cba.omnia.parlour.SqoopSyntax.ParlourExportDsl
  type TeradataParlourImportDsl = au.com.cba.omnia.parlour.SqoopSyntax.TeradataParlourImportDsl
  type TeradataParlourExportDsl = au.com.cba.omnia.parlour.SqoopSyntax.TeradataParlourExportDsl
  val  ParlourImportDsl         = au.com.cba.omnia.parlour.SqoopSyntax.ParlourImportDsl
  val  ParlourExportDsl         = au.com.cba.omnia.parlour.SqoopSyntax.ParlourExportDsl
  val  TeradataParlourImportDsl = au.com.cba.omnia.parlour.SqoopSyntax.TeradataParlourImportDsl
  val  TeradataParlourExportDsl = au.com.cba.omnia.parlour.SqoopSyntax.TeradataParlourExportDsl
  type ParlourImportOptions[+T <: ParlourImportOptions[_]] = au.com.cba.omnia.parlour.ParlourImportOptions[T]
  type ParlourExportOptions[+T <: ParlourExportOptions[_]] = au.com.cba.omnia.parlour.ParlourExportOptions[T]
  type SqoopImportConfig[T <: ParlourImportOptions[T]]     = au.com.cba.omnia.maestro.core.exec.SqoopImportConfig[T]
  type SqoopExportConfig[T <: ParlourExportOptions[T]]     = au.com.cba.omnia.maestro.core.exec.SqoopExportConfig[T]
  val  SqoopImportConfig                                   = au.com.cba.omnia.maestro.core.exec.SqoopImportConfig
  val  SqoopExportConfig                                   = au.com.cba.omnia.maestro.core.exec.SqoopExportConfig

  val  ParlourInstances                       = au.com.cba.omnia.maestro.core.exec.ParlourInstances
  implicit val parlourImportDslMonoid         = ParlourInstances.parlourImportDslMonoid
  implicit val parlourExportDslMonoid         = ParlourInstances.parlourExportDslMonoid
  implicit val teradataParlourImportDslMonoid = ParlourInstances.teradataParlourImportDslMonoid
  implicit val teradataParlourExportDslMonoid = ParlourInstances.teradataParlourExportDslMonoid

  val  SqoopExecutionTest = au.com.cba.omnia.maestro.core.exec.SqoopExecutionTest
}
