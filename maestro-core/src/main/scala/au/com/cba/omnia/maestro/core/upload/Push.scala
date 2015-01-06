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
package upload

import java.io.File

import org.apache.commons.compress.compressors.gzip.GzipCompressorOutputStream

import org.apache.hadoop.fs.Path

import com.google.common.io.Files

import au.com.cba.omnia.omnitool.Result
import au.com.cba.omnia.omnitool.file.ops.Temp

import au.com.cba.omnia.permafrost.hdfs.Hdfs

/** File copied to HDFS */
case class Copied(source: File, dest: Path)

/** Functions to push files to HDFS */
object Push {

  /**
    * Copy the file to HDFS and archive it.
    *
    * If nothing goes wrong, this methods will:
    *   - copy the file to Hdfs
    *   - create an "ingestion complete" flag on Hdfs
    *   - archive the file
    *
    * This method will fail if a duplicate of the file or it's
    * ingestion complete or processed flags already exist in HDFS,
    * or if an archived copy of the file already exists locally or on HDFS.
    *
    * This method is not atomic. It assumes no other process is touching the
    * source and multiple destination files while this method runs.
    */
  def push(src: Data, hdfsLandingDir: String, archiveDir: String, hdfsArchiveDir: String): Hdfs[Copied] = {
    val fileName = src.file.getName
    val archName = fileName + ".gz"
    val hdfsDestDir      = Hdfs.path(List(hdfsLandingDir, src.fileSubDir) mkString File.separator)
    val hdfsDestFile     = new Path(hdfsDestDir, fileName)
    val hdfsPushFlagFile = new Path(hdfsDestDir, "_INGESTION_COMPLETE")
    val hdfsProcFlagFile = new Path(hdfsDestDir, "_PROCESSED")
    val archDestDir      = new File(List(archiveDir, src.fileSubDir) mkString File.separator)
    val archDestFile     = new File(archDestDir, archName)
    val archHdfsDestDir  = new Path(List(hdfsArchiveDir, src.fileSubDir) mkString File.separator)
    val archHdfsDestFile = new Path(archHdfsDestDir, archName)

    for {
      // fail if any traces of the file already exist on HDFS
      _ <- Hdfs.forbidden(Hdfs.exists(hdfsDestFile),     s"$fileName already exists at $hdfsDestFile")
      _ <- Hdfs.forbidden(Hdfs.exists(hdfsPushFlagFile), s"$fileName already has an ingestion complete flag at $hdfsPushFlagFile")
      _ <- Hdfs.forbidden(Hdfs.exists(hdfsProcFlagFile), s"$fileName already has a processed flag at $hdfsProcFlagFile")
      _ <- Hdfs.forbidden(Hdfs.exists(archHdfsDestFile), s"$fileName has already been archived to $archHdfsDestFile")
      _ <- Hdfs.prevent(archDestFile.exists,             s"$fileName has already been archived to $archDestFile")

      // push file everywhere and create flag
      _ <- copyToHdfs(src.file, hdfsDestDir)
      _ <- Hdfs.create(hdfsPushFlagFile)
      _ <- archiveFile(src.file, archDestDir, archHdfsDestDir, archName)
      _ <- Hdfs.guard(src.file.delete, s"could not delete $fileName after processing")

    } yield Copied(src.file, hdfsDestFile)
  }

  /** Compress and archive file both locally and in hdfs */
  def archiveFile(raw: File, archDestDir: File, archHdfsDestDir: Path, archName: String): Hdfs[Unit] =
    hdfsWithTempFile(raw, archName, compressed => for {
      _ <- copyToHdfs(compressed, archHdfsDestDir)
      _ <- moveToDir(compressed, archDestDir)
    } yield () )

  /** Copy file to hdfs */
  def copyToHdfs(file: File, destDir: Path): Hdfs[Unit] =
    for {
      // mkdirs returns true if dir already exists
      _ <- Hdfs.mandatory(Hdfs.mkdirs(destDir), s"$destDir could not be created")
      _ <- Hdfs.copyFromLocalFile(file, destDir)
    } yield ()

  /** Move file to another directory. Convienient to run in a Hdfs operation. */
  def moveToDir(file: File, destDir: File): Hdfs[Unit] =
    Hdfs.result(
      for {
        _ <- Result.guard(destDir.isDirectory || destDir.mkdirs, s"$destDir could not be created")
        _ <- Result.ok[Unit](Files.move(file, new File(destDir, file.getName)))
      } yield ()
    )

  /** Convert a Hdfs operation that depends on a temporary file into a normal Hdfs operation */
  def hdfsWithTempFile[A](raw: File, fileName: String, action: File => Hdfs[A]): Hdfs[A] =
    Hdfs(c => Temp.withTempModified(
      raw, fileName, new GzipCompressorOutputStream(_), action(_).run(c)
    ))
}
