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

package au.com.cba.omnia.maestro.core.task

import java.nio.ByteBuffer
import java.security.{MessageDigest, SecureRandom}
import java.util.UUID

import scala.reflect.runtime.currentMirror
import scala.util.control.NonFatal
import scala.util.hashing.MurmurHash3

import scalaz.{Tag => _, _}, Scalaz._

import org.apache.hadoop.io.{BytesWritable, NullWritable}
import org.apache.hadoop.fs.{FileSystem, Path}

import cascading.flow.FlowProcess
import cascading.operation.{BaseOperation, Function, FunctionCall, OperationCall}
import cascading.tap.hadoop.io.MultiInputSplit.CASCADING_SOURCE_PATH
import cascading.tuple.Tuple

import com.twitter.scalding.{Execution, ExecutionCounters, MultipleTextLineFiles, TypedPipe, Stat}
import com.twitter.scalding.typed.TypedPipeFactory
import com.twitter.scalding.Dsl._ // Pipe.eachTo

import com.twitter.bijection.scrooge.CompactScalaCodec

import com.twitter.scrooge.{ThriftStruct, ThriftStructCodec}

import au.com.cba.omnia.maestro.core.codec._
import au.com.cba.omnia.maestro.core.clean.Clean
import au.com.cba.omnia.maestro.core.filter.RowFilter
import au.com.cba.omnia.maestro.core.scalding._
import au.com.cba.omnia.maestro.core.split.Splitter
import au.com.cba.omnia.maestro.core.time.TimeSource
import au.com.cba.omnia.maestro.core.validate.Validator
import au.com.cba.omnia.maestro.core.scalding.ExecutionOps._

import au.com.cba.omnia.permafrost.hdfs.Hdfs

/** Information about a Load */
sealed trait LoadInfo {
  /** Should we continue the load? */
  def continue: Boolean = this match {
    case LoadSuccess(_, _, _, _) => true
    case _                       => false
  }

  /**
    * Returns the successfull load stats if we should continue the load.
    * Fails if we should not continue
    */
  def withSuccess: Execution[LoadSuccess] = Execution.from(this match {
    case ls@LoadSuccess(_, _, _, _) => ls
    case _                          => throw new Exception(s"Unsuccessfull load. Load info = $this")
  })
}

/** The Load had no new data to process */
case object EmptyLoad extends LoadInfo

/**
  * The number of rows which failed to load exceeded our error threshold
  *
  * @param actual: the number of rows that remained after filtering.
  */
case class LoadFailure(read: Long, actual: Long, written: Long, failed: Long) extends LoadInfo

/**
  * The number of rows which failed to load was within acceptable limits
  *
  * @param actual: the number of rows that remained after filtering.
  */
case class LoadSuccess(read: Long, actual: Long, written: Long, failed: Long) extends LoadInfo

/** Factory methods for `LoadInfo` */
object LoadInfo {
  def fromCounters(counters: ExecutionCounters, errorThreshold: Double): LoadInfo = {
    // If there is no data, nothing will be read in and this counter won't be set
    val read     = counters.get(StatKeys.tuplesRead).getOrElse(0l)
    // If no values are filtered this counter will be 0
    val filtered = counters.get(StatKeys.tuplesFiltered).getOrElse(0l)
    // If all values fail nothing will be written out and this counter won't be set
    val written  = counters.get(StatKeys.tuplesWritten).getOrElse(0l)

    // The number of rows that remain after filtering
    val actual   = read - filtered
    // If all values pass nothing will be written out and this counter won't be set
    val failed   = counters.get(StatKeys.tuplesTrapped).getOrElse(actual - written)

    if (actual == 0)                                     EmptyLoad
    else if (failed == 0)                                LoadSuccess(read, actual, written, failed)
    else if (failed / actual.toDouble >= errorThreshold) LoadFailure(read, actual, written, failed)
    else                                                 LoadSuccess(read, actual, written, failed)
  }
}

/**
  * Configuration options for load
  *
  * @param errors:         The hdfs path used to store rows which failed processing.
  * @param splitter:       The `Splitter` used to split rows into fields.
  *                        Defaults to splitting on the "|" character.
  * @param timeSource:     The `TimeSource` used to find the effective date for each row.
  *                        Defaults to pulling the year, month, and day from the
  *                        directory structure of the input file.
  * @param generateKey:    If true, load will append a unique key to each row.
  *                        Defaults to false.
  * @param filter:         The `RowFilter` which scans each row after splitting,
  *                        potentially discarding bad rows. Defaults to now filtering.
  * @param clean:          The `Clean` function to be applied to each field.
  *                        Defaults to trimming fields and removing non-printable characters.
  * @param none:           The field value denoting a `None` value for an optional field.
  *                        Optional string fields are `None` only if they match this value.
  *                        Other optional fields are `None` if they match this or the empty string.
  *                        Defaults to the empty sring.
  * @param validator:      The `Validator` to check the thrift struct to ensure it is correct.
  *                        Defaults to no validation.
  * @param errorThreshold: The fraction of rows that can fail during the load step
  *                        before the overall job should fail. Defaults to 0.05 or 5%.
  * @param splitSize:      The size of each split in the job that processes the
  *                        pipe returned by load. Defaults to 9 blocks, with the
  *                        hope that this will result in 2-block parquet files.
  */
case class LoadConfig[A](
  errors: String,
  splitter: Splitter      = Splitter.delimited("|"),
  timeSource: TimeSource  = TimeSource.fromDirStructure,
  generateKey: Boolean    = false,
  filter: RowFilter       = RowFilter.keep,
  clean: Clean            = Clean.default,
  none: String            = "",
  validator: Validator[A] = Validator.all[A](),
  errorThreshold: Double  = 0.05,
  splitSize: Long         = 9L * 128 * 1024 * 1024
)

/** Executions for load tasks */
trait LoadExecution {
  /**
    * Loads the supplied text files and converts them to the specified thrift struct.
    *
    * The operations performed are:
    *  1. Append a time field to each line using the provided time source
    *  1. Split each line into columns/fields using the provided splitter
    *  1. Apply the provided filter to each list of fields (defaults to no filtering).
    *  1. Clean each field using the provided cleaner
    *  1. Convert the list of fields into the provided thrift struct.
    *  1. Validate each struct (defaults to assuming all structs are ok).
    *  1. The fraction of successfull rows is compared against the error threshold
    *     and the appropriate `LoadInfo` value returned.
    */
  def load[A <: ThriftStruct : Decode : Tag : Manifest](
    config: LoadConfig[A], sources: List[String]
  ): Execution[(TypedPipe[A], LoadInfo)] = {
    val resolvePath = (srcPath: Path) =>
      for {
        isDir <- Hdfs.isDirectory(srcPath)
        paths <- if (!isDir) Hdfs.value(List(srcPath))
                 else Hdfs.files(srcPath)
      } yield paths

    for {
      srcsResolved <- Execution.fromHdfs(
                        for {
                          fs       <- Hdfs.filesystem
                          paths    =  sources.map(Hdfs.path(_))
                          resPaths <- paths.map(resolvePath).sequence
                        } yield resPaths.flatten
                      )
      (pipe, info) <- LoadEx.execution[A](config, srcsResolved.map(_.toString))
    } yield (pipe, info)
  }
}

/**
  * WARNING: not considered part of the load execution api.
  * We may change this without considering backwards compatibility.
  */
object LoadEx {

  def execution[A <: ThriftStruct : Decode : Tag : Manifest](
    config: LoadConfig[A], sources: List[String]
  ): Execution[(TypedPipe[A], LoadInfo)] = {
    val rawRows =
      if (config.generateKey) pipeWithDateAndKey(sources, config.timeSource)
      else                    pipeWithDate(sources, config.timeSource)

    Execution.withId { id => Paths.tempDir.flatMap { tmpDir => {
      val stat        = Stat(StatKeys.tuplesFiltered)(id)
      val parsedRows  = parseRows(config, stat, rawRows)
      val thriftInput = Errors.safely(config.errors)(parsedRows)

      val seqFile     = tmpDir + "/maestro/" + UUID.randomUUID + ".seq"
      val seqSource   = CombinedSequenceFile[NullWritable, BytesWritable](seqFile, config.splitSize)

      val serializer  = CompactScalaCodec[A](getCodec[A])
      val seqFileIn   = thriftInput.map(t => (NullWritable.get, new BytesWritable(serializer(t))))
      val seqFileOut  = TypedPipe.from[(NullWritable, BytesWritable)](seqSource)
                          .map { case (_, bytes) => serializer.invert(bytes.getBytes).get }

      seqFileIn.writeExecution(seqSource).getAndResetCounters.map { case (_, counters) =>
        (seqFileOut, LoadInfo.fromCounters(counters, config.errorThreshold))
      }
    }}}
  }

  def parseRows[A <: ThriftStruct : Decode : Tag : Manifest](
    conf: LoadConfig[A], filterCounter: Stat, in: TypedPipe[RawRow]
  ): TypedPipe[String \/ A] = {
    val pipe = TypedPipeFactory { (flowDef, mode) =>
      implicit val fd = flowDef
      implicit val m  = mode

      TypedPipe.from[(RawRow, FlowProcess[_])](
        in.toPipe('row).eachTo('row, ('row, 'fp))(_ => new AddFlowProcess()),
        ('row, 'fp)
      )
    }

    pipe
      .map { case (row, fp) => (conf.splitter.run(row.line) ++ row.extraFields, fp) }
      .flatMap { case (parts, fp) => conf.filter.run(parts) match {
        case Some(r) => Some(r)
        case None    => {
          // Uses Cascading counter instead of Scalding. See `AddFlowProcess` for details.
          fp.increment("maestro", "tuples_filtered", 1L)
          None
        }
      }}.map(record =>
        Tag.tag[A](record).map { case (column, field) => conf.clean.run(field, column) }
      ).map(record => Decode.decode[A](conf.none, record))
        .map {
        case DecodeOk(value) =>
          conf.validator.run(value).disjunction.leftMap(errors => s"""The following errors occured: ${errors.toList.mkString(",")}""")
        case e @ DecodeError(remainder, counter, reason) =>
          reason match {
            case ParseError(value, expected, error) =>
              s"unexpected type: $e".left
            case NotEnoughInput(required, expected) =>
              s"not enough fields in record: $e".left
            case TooMuchInput =>
              s"too many fields in record: $e".left
          }
      }
    }

  def pipeWithDate(sources: List[String], timeSource: TimeSource): TypedPipe[RawRow] =
    TypedPipeFactory { (flowDef, mode) =>
      TypedPipe.fromSingleField[RawRow](
        MultipleTextLineFiles(sources: _*)
          .read(flowDef, mode)
          .eachTo(('offset, 'line), 'result)(_ => new ExtractTime(timeSource))
      )(flowDef, mode)
    }

  def pipeWithDateAndKey(sources: List[String], timeSource: TimeSource): TypedPipe[RawRow] = {
    val rnd    = new SecureRandom()
    val seed   = rnd.generateSeed(4)
    val md     = MessageDigest.getInstance("SHA-1")
    val hashes =
      sources
        .map(k => k -> md.digest(seed ++ k.getBytes("UTF-8")).drop(12).map("%02x".format(_)).mkString)
        .toMap

    TypedPipeFactory { (flowDef, mode) =>
      TypedPipe.fromSingleField[RawRow](
        MultipleTextLineFiles(sources: _*)
          .read(flowDef, mode)
          .eachTo(('offset, 'line), 'result)(_ => new GenerateKey(timeSource, hashes))
      )(flowDef, mode)
    }
  }

  /* By convention, all ThriftStruct classes are created with a ThriftStructCodec
   * companion object. Annoyingly, however, the ThriftStruct constraint does not
   * allow us to access that object. For the moment, we are using unsafe runtime
   * reflection to access the companion object.
   *
   * Using a compile-time method would be preferable, but would require us to add
   * extra constraints on A, breaking backwards compatibility slightly. We should
   * do this sometime in the future.
   */
  def getCodec[A <: ThriftStruct : Manifest]: ThriftStructCodec[A] = {
    val klazz = manifest[A].runtimeClass
    try {
      val companionSyb = currentMirror.classSymbol(klazz).companionSymbol
      val companion    = currentMirror.reflectModule(companionSyb.asModule).instance
      companion.asInstanceOf[ThriftStructCodec[A]]
    } catch {
      case NonFatal(e) => throw new IllegalArgumentException(s"Cannot find ThriftStructCodec companion object for ${klazz.getName}", e)
    }
  }
}

case class RawRow(line: String, extraFields: List[String])

/**
  * Used by `Load.loadWithKey` to generates a unique key for each input line.
  *
  * It hashes each line and combines that with the hash of the path and the slice number
  * (map task number) to create a unique key for each line.
  * It also gets the path from cascading and extracts the time from that using the provided
  * `timeSource`.
  */
class GenerateKey(timeSource: TimeSource, hashes: Map[String, String])
    extends BaseOperation[(ByteBuffer, MessageDigest, String, Tuple)](('result))
    with Function[(ByteBuffer, MessageDigest, String, Tuple)] {
  /** Creates a unique key from the provided information.*/
  def uid(path: String, slice: Int, offset: Long, line: String, byteBuffer: ByteBuffer, md: MessageDigest): String = {
    val hash     = hashes(path)
    val lineHash = md.digest(line.getBytes("UTF-8")).drop(8)

    byteBuffer.clear

    val lineInfo = byteBuffer.putInt(slice).putLong(offset).array
    val hex      = (lineInfo ++ lineHash).map("%02x".format(_)).mkString

    s"$hash$hex"
  }

  /**
    * Sets up the follow on processing by initialising the hashing algorithm, getting the path from
    * cascading and creating a reusable tuple.
    */
  override def prepare(
    flow: FlowProcess[_],
    call: OperationCall[(ByteBuffer, MessageDigest, String, Tuple)]
  ): Unit = {
    val md   = MessageDigest.getInstance("SHA-1")
    val path = flow.getProperty(CASCADING_SOURCE_PATH).toString
    call.setContext((ByteBuffer.allocate(12), md, path, Tuple.size(1)))
  }

  /** Operates on each line to create a unique key and extract the time from the path.*/
  def operate(flow: FlowProcess[_], call: FunctionCall[(ByteBuffer, MessageDigest, String, Tuple)])
      : Unit = {
    val entry  = call.getArguments
    val offset = entry.getLong(0)
    val line   = entry.getString(1)
    val slice  = flow.getCurrentSliceNum

    val (byteBuffer, md, path, resultTuple) = call.getContext
    val time = timeSource.getTime(path)
    val key  = uid(path, slice, offset, line, byteBuffer, md)

    // representation of RawRow: tuple with string and List elements
    resultTuple.set(0, RawRow(line, List(time, key)))
    call.getOutputCollector.add(resultTuple)
  }
}

/** Gets the path from Cascading and provides it to `timeSource` to get the time.*/
class ExtractTime(timeSource: TimeSource)
    extends BaseOperation[(String, Tuple)](('result))
    with Function[(String, Tuple)] {
  /**
    * Sets up the follow on processing by initialising the hashing algorithm, getting the path from
    * cascading and creating a reusable tuple.
    */
  override def prepare(
    flow: FlowProcess[_],
    call: OperationCall[(String, Tuple)]
  ): Unit = {
    val path = flow.getProperty(CASCADING_SOURCE_PATH).toString
    call.setContext((path, Tuple.size(1)))
  }

  /** Operates on each line to extract the time from the path.*/
  def operate(flow: FlowProcess[_], call: FunctionCall[(String, Tuple)])
      : Unit = {
    val line                = call.getArguments.getString(1)
    val (path, resultTuple) = call.getContext

    // representation of RawRow: tuple with string and List elements
    resultTuple.set(0, RawRow(line, List(timeSource.getTime(path))))
    call.getOutputCollector.add(resultTuple)
  }
}

/**
  * Drops down to the Cascading level to get the FlowProcess.
  *
  * We do this so that we can then use the flow process later to increment a counter.
  * The Scalding way of doing this seems to be broken see
  * <a href="https://github.com/CommBank/maestro/issues/300">300</a>.
  */
class AddFlowProcess extends BaseOperation[Tuple](('row, 'fp)) with Function[Tuple] {
  override def prepare(flow: FlowProcess[_], call: OperationCall[Tuple]): Unit = {
    call.setContext(Tuple.size(2))
  }

  def operate(flow: FlowProcess[_], call: FunctionCall[Tuple]): Unit = {
    val row         = call.getArguments.getObject(0)
    val resultTuple = call.getContext

    resultTuple.set(0, row)
    resultTuple.set(1, flow)
    call.getOutputCollector.add(resultTuple)
  }
}
