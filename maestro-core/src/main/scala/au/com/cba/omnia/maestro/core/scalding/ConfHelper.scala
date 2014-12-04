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
package scalding

import org.apache.hadoop.mapred.JobConf

import com.twitter.scalding.Config

object ConfHelper {
  def getHadoopConf(config: Config): JobConf = {
    val conf = new JobConf(true)
    config.toMap.foreach{ case (k, v) => conf.set(k, v) }
    conf
  }
}
