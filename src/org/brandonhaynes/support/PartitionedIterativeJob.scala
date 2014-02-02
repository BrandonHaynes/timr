package org.brandonhaynes.support

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.io.Writable
import org.apache.hadoop.mapreduce
import org.apache.hadoop.mapreduce.InputFormat

/** Base class for one step an iterative job that is executed with a fixed number of reducers.  Serves as a lightweight
  * wrapper over a standard Hadoop job by setting some properties (e.g., mapper, reducer, input format) specifying
  * some sane defaults (number of reduce tasks, job name) and setting configuration useful for iterative jobs (e.g.,
  * total number of steps in the job, current step number).
  *
  * @param configurationTemplate Template for job configuration; these values are copied for the new job
  * @param name Name of the job
  * @param partitions Fixed number of reducers for the job
  * @param step Step number of this job within the iterative process
  * @param steps Total number of steps in the iterative process
  * @tparam InputKey Type of key input to this step of the job
  * @tparam InputValue Type of value input to this step of the job
  * @tparam IntermediateKey Intermediate key type for this step of the job (output from mapper, input to reducer)
  * @tparam IntermediateValue Intermediate value type for this step of the job (output from mapper, input to reducer)
  * @tparam OutputKey Key type output from this step of the job
  * @tparam OutputValue Value type output from this step of the job
  * @tparam Mapper Concrete Mapping type
  * @tparam Reducer Concrete reducer type
  * @tparam Partitioner Partitioner type for the job
  * @tparam Format Input format for data fed to this step of the job
  */
class PartitionedIterativeJob[InputKey <: Writable, InputValue <: Writable,
                              IntermediateKey, IntermediateValue,
                              OutputKey : Manifest, OutputValue : Manifest,
                              Mapper <: mapreduce.Mapper[InputKey, InputValue,
                                                         IntermediateKey, IntermediateValue] : Manifest,
                              Reducer <: mapreduce.Reducer[IntermediateKey, IntermediateValue,
                                                           OutputKey, OutputValue] : Manifest,
                              Partitioner <: mapreduce.Partitioner[IntermediateKey, IntermediateValue] : Manifest,
                              Format <: InputFormat[InputKey, InputValue] : Manifest](
    configurationTemplate:Configuration, name:String, partitions:Int, step:Int, steps:Int)
  extends IterativeJob[InputKey, InputValue, IntermediateKey, IntermediateValue, OutputKey, OutputValue,
                       Mapper, Reducer, Format](configurationTemplate, name, Some(partitions), step, steps)
  { setPartitionerClass(manifest[Partitioner].runtimeClass.asInstanceOf[Class[Partitioner]]) }

/** Represents a composable job with (i) a fixed number of reducers, (ii) that is one step of an iterative map/reduce
  * process, and (iii) where the key and value types are the same across all steps of the job.  Serves as a lightweight
  * wrapper over a standard Hadoop job by setting some properties (e.g., mapper, reducer, input format) specifying some
  * sane defaults (number of reduce tasks, job name) and setting configuration useful for iterative jobs (e.g., total
  * number of steps in the job, current step number).
  *
  * @param configurationTemplate Template for job configuration; these values are copied for the new job
  * @param name Name of the job
  * @param partitions Fixed number of reducers for the job
  * @param step Step number of this job within the iterative process
  * @param steps Total number of steps in the iterative process
  * @tparam Key Type of key used for the input, intermediate output, and final output for this step of the job
  * @tparam Value Type of key used for the input, intermediate output, and final output for this step of the job
  * @tparam Mapper Concrete Mapping type
  * @tparam Reducer Concrete reducer type
  * @tparam Format Input format for data fed to this step of the job
  */
class ComposablePartitionedIterativeJob[Key <: Writable : Manifest, Value <: Writable : Manifest,
                                        Mapper <: mapreduce.Mapper[Key, Value, Key, Value] : Manifest,
                                        Reducer <: mapreduce.Reducer[Key, Value, Key, Value] : Manifest,
                                        Partitioner <: mapreduce.Partitioner[Key, Value] : Manifest,
                                        Format <: InputFormat[Key, Value] : Manifest](
    configurationTemplate:Configuration, name:String, partitions:Int, step:Int, steps:Int)
  extends PartitionedIterativeJob[Key, Value, Key, Value, Key, Value, Mapper, Reducer, Partitioner, Format](
    configurationTemplate, name, partitions, step, steps) { }
