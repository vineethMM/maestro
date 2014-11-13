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

import au.com.cba.omnia.maestro.macros.MacroSupport

/**
  * Extend this trait to get access to all maestro execution tasks, and supporting types.
  *
  * Contains:
  *  - Maestro execution tasks, such as `upload`, `load`, `view`, etc.
  *  - Supporting types required to call these tasks, such as `Validator`, `Partition`, etc.
  *  - The scalding `Execution` type
  *  - Support for dealing with your thrift type `A`.
  */
trait MaestroExecution[A <: ThriftStruct] extends MaestroExecutionBase with MacroSupport[A]

/**
  * Use this object to get access to all maestro execution tasks, and supporting types.
  *
  * Contains:
  *  - Maestro execution tasks, such as `upload`, `load`, `view`, etc.
  *  - Supporting types required to call these tasks, such as `Validator`, `Partition`, etc.
  *  - The scalding `Execution` type
  */
object MaestroExecution extends MaestroExecutionBase
