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

package au.com.cba.omnia.maestro.api.exec

import com.twitter.scrooge.ThriftStruct

import au.com.cba.omnia.maestro.core.exec.MaestroExecutionMain
import au.com.cba.omnia.maestro.macros.MacroSupport

/**
  * Create an object extending this trait to create Maestro Job using the execution api.
  *
  * Objects extending this trait must supply a:
  *  - job execution: this is the main execution to run for the Maestro job
  *  - attemptsExceeded execution: this execution should be run if the job execution fails too many times.
  *  - logger: This will be used to log error messages from the above two executions.
  *
  * The job will exit with an error code describing the status of the program.
  * The software running this program can then take the appropriate action.
  */
trait MaestroJob[A <: ThriftStruct] extends MaestroExecutionMain with MacroSupport[A]
