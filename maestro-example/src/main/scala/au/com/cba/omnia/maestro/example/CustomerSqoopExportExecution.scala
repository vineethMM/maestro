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

import com.twitter.scalding._

import au.com.cba.omnia.maestro.api.exec.MaestroExecution
import au.com.cba.omnia.maestro.example.thrift.Customer

import au.com.cba.omnia.parlour.SqoopSyntax.ParlourExportDsl

/** Please be aware that the Execution API is being actively developed/modified and
  * hence not officially supported or ready for production use yet.
  */
object CustomerSqoopExportExecution extends MaestroExecution[Customer] {
  def execute(
    hdfsRoot: String,
    connectionString: String, 
    username: String, 
    password: String,
    options: ParlourExportDsl
  ) : Execution[Unit] = {
    val rawDataDir    = s"$hdfsRoot/customers"
    val exportDir     = s"$hdfsRoot/processed-customers"
    val tableName     = "customer_export"
    val nullString    = ""
  
    for {
      _ <- {
        TypedPipe.from(TextLine(rawDataDir))
          .map(Splitter.delimited("|").run(_).init.mkString("|"))
          .writeExecution(TypedPsv(exportDir))
      }
      _ <- sqoopExport(exportDir, tableName, connectionString, username, password, '|', nullString, options)
    } yield ()
  }
}


