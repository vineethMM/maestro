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

import org.specs2.Specification

import java.io.File

import org.apache.hadoop.fs.Path
import org.apache.hadoop.conf.Configuration

import au.com.cba.omnia.omnitool.{Error, Ok}

import au.com.cba.omnia.permafrost.hdfs.Hdfs

class PushSpec extends Specification { def is = s2"""

Push properties
===============

archive behaviour
-----------------

  archive puts file in archive dir   $archivesFile
  archive puts file in hdfs arch dir $archivesInHdfs
  archive puts file in sub directory $archivesInSubDir

push behaviour
--------------

  push copies to HDFS          $pushCopiesFile
  push copies SAP file to HDFS $pushCopiesSapFile
  push creates ingestion file  $pushCreatesIngestionFile
  push fails on ingestion file $pushFailsOnIngestionFile
  push fails on archive file   $pushFailsOnArchiveFile
  push fails on duplicates     $pushFailsOnDuplicate
  push removes source file     $pushRemovesSourceFile

"""

  def archivesFile = isolatedTest((dirs: IsolatedDirs) => {
    val local = new File(dirs.testDir, "local.txt")
    local.createNewFile must_== true

    val archiveCheck = Push.archiveFile(local, dirs.archiveDir,
      dirs.hdfsArchiveDirP, "local.txt.gz")
      .run(new Configuration)
    archiveCheck mustEqual Ok(())

    val destFile = new File(dirs.archiveDir, "local.txt.gz")
    destFile.isFile must beTrue
  })

  def archivesInHdfs = isolatedTest((dirs: IsolatedDirs) => {
    val local = new File(dirs.testDir, "local.txt")
    local.createNewFile must_== true

    val archiveCheck = Push.archiveFile(local, dirs.archiveDir,
      dirs.hdfsArchiveDirP, "local.txt.gz")
      .run(new Configuration)
    archiveCheck mustEqual Ok(())

    val destFile = new File(dirs.hdfsArchiveDir, "local.txt.gz")
    destFile.isFile must beTrue
  })

  def archivesInSubDir = isolatedTest((dirs: IsolatedDirs) => {
    val local = new File(dirs.testDir, "local.txt")
    local.createNewFile must_== true
    val destDir = new File(dirs.archiveDir, "subDir")

    val archiveCheck = Push.archiveFile(local, destDir, dirs.hdfsArchiveDirP,
      "local.txt.gz")
      .run(new Configuration)
    archiveCheck mustEqual Ok(())

    val destFile = new File(destDir, "local.txt.gz")
    destFile.isFile must beTrue
  })

  def pushCopiesFile = isolatedTest((dirs: IsolatedDirs) => {
    val src  = Data(new File(dirs.testDir, "local20140506.txt"), new File("foo/bar"))
    val dest = new Path(List(dirs.hdfsDirS, "foo", "bar", "local20140506.txt") mkString File.separator)
    val conf = new Configuration
    src.file.createNewFile must_== true

    val copyCheck = Push.push(src, dirs.hdfsDirS, dirs.archiveDirS,
      dirs.hdfsArchiveDirS)
      .run(conf)
    copyCheck mustEqual Ok(Copied(src.file, dest))

    Hdfs.exists(dest).run(conf) mustEqual Ok(true)
  })

  def pushCopiesSapFile = isolatedTest((dirs: IsolatedDirs) => {
    val src  = Data(new File(dirs.testDir, "ZCR_DW01_E001_20140612_230441.DAT"), new File("."))
    val dest = new Path(dirs.hdfsDirS + File.separator + "ZCR_DW01_E001_20140612_230441.DAT")
    val conf = new Configuration
    src.file.createNewFile must_== true

    val copyCheck = Push.push(src, dirs.hdfsDirS, dirs.archiveDirS,
      dirs.hdfsArchiveDirS)
      .run(conf)
    copyCheck mustEqual Ok(Copied(src.file, dest))

    Hdfs.exists(dest).run(conf) mustEqual Ok(true)
  })

  def pushCreatesIngestionFile = isolatedTest((dirs: IsolatedDirs) => {
    val src  = Data(new File(dirs.testDir, "local20140506.txt"), new File("."))
    val dest = new Path(dirs.hdfsDirS + File.separator + "local20140506.txt")
    val flag = new Path(dirs.hdfsDirS + File.separator + "_INGESTION_COMPLETE")
    val conf = new Configuration
    src.file.createNewFile must_== true

    val copyCheck = Push.push(src, dirs.hdfsDirS, dirs.archiveDirS,
      dirs.hdfsArchiveDirS)
      .run(conf)
    copyCheck mustEqual Ok(Copied(src.file, dest))

    Hdfs.exists(flag).run(conf) mustEqual Ok(true)
  })

  def pushFailsOnIngestionFile = isolatedTest((dirs: IsolatedDirs) => {
    val src  = Data(new File(dirs.testDir, "local20140506.txt"), new File("."))
    val dest = new Path(dirs.hdfsDirS + File.separator + "local20140506.txt")
    val flag = new Path(dirs.hdfsDirS + File.separator + "_INGESTION_COMPLETE")
    val conf = new Configuration
    src.file.createNewFile must_== true
    Hdfs.create(flag).run(conf) must beLike { case Ok(_) => ok }

    val copyCheck = Push.push(src, dirs.hdfsDirS, dirs.archiveDirS,
      dirs.hdfsArchiveDirS)
      .run(conf)
    copyCheck must beLike { case Error(_) => ok }

    Hdfs.exists(dest).run(conf) mustEqual Ok(false)
  })

  def pushFailsOnArchiveFile = isolatedTest((dirs: IsolatedDirs) => {
    val src  = Data(new File(dirs.testDir, "local20140506.txt"), new File("."))
    val dest = new Path(dirs.hdfsDirS + File.separator + "local20140506.txt")
    val arch = new Path(dirs.hdfsArchiveDirS + File.separator + "local20140506.txt.gz")
    val conf = new Configuration
    src.file.createNewFile must_== true
    Hdfs.create(arch).run(conf) must beLike { case Ok(_) => ok }

    val copyCheck = Push.push(src, dirs.hdfsDirS, dirs.archiveDirS,
      dirs.hdfsArchiveDirS)
      .run(conf)
    copyCheck must beLike { case Error(_) => ok }

    Hdfs.exists(dest).run(conf) mustEqual Ok(false)
  })

  def pushFailsOnDuplicate = isolatedTest((dirs: IsolatedDirs) => {
    val src  = Data(new File(dirs.testDir, "local20140506.txt"), new File("."))
    val dest = new Path(dirs.hdfsDirS + File.separator + "local20140506.txt")
    val conf = new Configuration
    src.file.createNewFile must_== true

    val copyCheck = Push.push(src, dirs.hdfsDirS, dirs.archiveDirS,
      dirs.hdfsArchiveDirS)
      .run(conf)
    copyCheck mustEqual Ok(Copied(src.file, dest))

    val duplicateCheck = Push.push(src, dirs.hdfsDirS, dirs.archiveDirS,
      dirs.hdfsArchiveDirS)
      .run(conf)
    duplicateCheck must beLike { case Error(_) => ok }
  })

  def pushRemovesSourceFile = isolatedTest((dirs: IsolatedDirs) => {
    val src  = Data(new File(dirs.testDir, "local20140506.txt"), new File("foo"))
    val dest = new Path(List(dirs.hdfsDirS, "foo", "local20140506.txt") mkString File.separator)
    val conf = new Configuration
    src.file.createNewFile must_== true

    val copyCheck = Push.push(src, dirs.hdfsDirS, dirs.archiveDirS,
      dirs.hdfsArchiveDirS)
      .run(conf)
    copyCheck mustEqual Ok(Copied(src.file, dest))

    src.file.exists mustEqual false
  })
}
