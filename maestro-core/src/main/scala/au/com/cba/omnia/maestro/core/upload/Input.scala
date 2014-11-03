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

import scala.util.matching.Regex

import scalaz._, Scalaz._

import au.com.cba.omnia.permafrost.hdfs.{Hdfs, Result}

/** A file existing in the source directory */
sealed trait Input

/** A control file, not to be loaded into HDFS */
case class Control(file: File) extends Input

/** A file to be loaded into HDFS */
case class Data(file: File, fileSubDir: File) extends Input

/** Factory for `Input`s */
object Input {

  /**
    * Find files from a directory matching a given file pattern.
    *
    * See the description of the file pattern at [[au.cba.com.omnia.meastro.core.task.Upload]]
    */
  def findFiles(sourceDir: File, tableName: String, pattern: String, controlPattern: Regex): Result[List[Input]] =
    Option(sourceDir.listFiles) match {
      case None        =>
        Result.fail(s"$sourceDir is not an existing directory")
      case Some(files) => for {
        inputMatcher <- fromDisjunction(InputParsers.forPattern(tableName, pattern))
        fileMatches  <- files.toList traverse (file =>
          fromDisjunction(inputMatcher(file.getName)) map ((m:MatchResult) => (file, m))
        )
      } yield for {
        (file, matchRes) <- fileMatches
        dirs             <- matchRes match {
          case Match(dirs) => List(dirs)
          case NoMatch     => List.empty[List[String]]
        }
        dirFile = new File(dirs mkString File.separator)
        input   = if (isControl(file, controlPattern)) Control(file) else Data(file, dirFile)
      } yield input
    }

  /** Convert a disjunction into a Result */
  def fromDisjunction[A](x: String \/ A) = x.fold(Result.fail(_), Result.ok(_))

  /** check if file matches any of the control file patterns */
  def isControl(file: File, controlPattern: Regex) =
    controlPattern.unapplySeq(file.getName).isDefined
}
