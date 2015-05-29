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

package au.com.cba.omnia.maestro.scalding

import au.com.cba.omnia.thermometer.core.ThermometerSpec

object MaestroExecutionSpec extends ThermometerSpec { def is = s2"""

Maestro Execution functions
===========================

Maestro Execution functions:
  can recover `JobNotReadyError`  $jobNotReady
  can recover `JobFailureError`   $jobFailure

"""

  def jobNotReady = {
    executesSuccessfully(MaestroExecution.recoverJobStatus(MaestroExecution.jobNotReady)) must_== JobNotReady
  }

  def jobFailure = {
    executesSuccessfully(MaestroExecution.recoverJobStatus(MaestroExecution.jobFailure(-2))) must_== JobFailure(-2)
  }
}
