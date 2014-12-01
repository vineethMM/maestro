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

package au.com.cba.omnia.maestro.core.exec

import org.slf4j.Logger

import scala.util.control.NonFatal

import com.twitter.scalding.{Config, Execution, ExecutionApp}

/** An enumeration representing possible program status */
sealed trait JobStatus {
  def exitCode: Int
}

/** The job succeeded and had work to do */
case object JobFinished extends JobStatus { val exitCode = 0 }

/** The job is not ready to run: it's pre-requisites are not available */
case object JobNotReady extends JobStatus { val exitCode = 1 }

/** The Job was never ready, and could not be retried any more */
case object JobNeverReady extends JobStatus { val exitCode = 2 }

/**
  * The job failed
  *
  * The exit code should be negative.
  */
case class JobFailure(override val exitCode: Int) extends JobStatus


/**
  * Create an object which extends this class and run the class as a normal java program.
  *
  * The program will exit with an appropriate error code describing the
  * status of the program. The script which runs this program can then take the
  * appropriate action.
  */
trait MaestroExecutionMain extends Serializable {
  /** The job to run */
  def job: Execution[JobStatus]

  /** Run if the job has failed too many times */
  def attemptsExceeded: Execution[JobStatus]

  /** The logger to use for this application */
  def logger: Logger

  def main(args: Array[String]) {
    val status = try {
      val (conf, mode) = ExApp.config(args)
      val execution    = if (conf.getArgs.boolean("attempts-exceeded")) attemptsExceeded else job
      execution.waitFor(conf, mode).get
    } catch {
      case NonFatal(ex) => {
        logger.error("error running execution", ex)
        JobFailure(-1)
      }
    }
    System.exit(status.exitCode)
  }

  private object ExApp extends ExecutionApp {
    def job = null // never used
  }
}
