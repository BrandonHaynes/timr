package org.brandonhaynes.timr.checkpoints

import org.apache.hadoop.io.Writable
import org.apache.hadoop.mapreduce
import org.brandonhaynes.timr.{TIMRJob, TIMRReducer}
import org.apache.hadoop.mapreduce.InputFormat
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat

/** Base class for a checkpointed TIMR-enabled map/reduce job
 *
 * @param configurationTemplate Template configuration from which to draw metadata
 * @param name Name of the job
 * @param partitions Number of partitions in the job
 * @param step Step number of this job in the iterative process
 * @param steps Total number of steps in the iterative job
 * @tparam Key Key used for the iterative job
 * @tparam Value Value used for the iterative job
 * @tparam Mapper Class containing the mapping function for this step of the iterative job
 * @tparam Reducer Class containing the reduction function for this step of the iterative job
 * @tparam Partitioner Partitioner for the iterative job
 * @tparam Format Input format for the iterative job
 */
class CheckpointTIMRJob[Key <: Writable : Manifest,
                        Value <: Writable : Manifest,
                        Mapper <: mapreduce.Mapper[Key, Value, Key, Value] : Manifest,
                        Reducer <: TIMRReducer[Key, Value] : Manifest,
                        Partitioner <: mapreduce.Partitioner[Key, Value] : Manifest,
                        Format <: InputFormat[Key, Value] : Manifest](
                           configurationTemplate:Configuration,
                           name:String,
                           partitions:Int,
                           step:Int,
                           steps:Int)
  extends TIMRJob[Key, Value, Mapper, Reducer, Partitioner, Format](
    configurationTemplate, name, partitions, step, steps) {

  // Since we are checkpointing, we need to filter out checkpoint files as input
  FileInputFormat.setInputPathFilter(this, classOf[CheckpointPathFilter])
}