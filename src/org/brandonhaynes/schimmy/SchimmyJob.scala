package org.brandonhaynes.schimmy

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.io.Writable
import org.apache.hadoop.mapreduce
import org.apache.hadoop.mapreduce.InputFormat
import org.brandonhaynes.support.ComposablePartitionedIterativeJob

/** Base class for a Schimmy-enabled map/reduce job (Lin & Dyer, 2010); here this job is assumed to be iterative and
  * composable (i.e., it has the same input and output key and value types) -- this is not strictly necessary for
  * Schimmy (intermediate types need only be a superset of input types), but is generally the case.
  *
  * The Schimmy pattern increases the performance of some map/reduce jobs by reducing the cost of the intermediate
  * shuffle.  It does so by performing a merge-join between the intermediate result and the original input to the
  * step; accordingly, static and (otherwise unmodified) pairs need not be emitted into the shuffle.  Since the
  * cost of shuffling exceeds that of the merge join, a performance gain is realized.
  *
  * This class automatically performs the Schimmy merge-join and presents the aggregate groups to each reducer
  * invocation.  Input pairs are yielded prior to intermediate pairs in the value iterator.
  *
  * Note that in order to properly perform the merge-join, it is necessary to fix the number of partitions; similarly,
  * the job input should be pre-partitioned by the same partition function.
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
class SchimmyJob[Key <: Writable : Manifest,
                 Value <: Writable : Manifest,
                 Mapper <: mapreduce.Mapper[Key, Value, Key, Value] : Manifest,
                 Reducer <: mapreduce.Reducer[Key, Value, Key, Value] : Manifest,
                 Partitioner <: mapreduce.Partitioner[Key, Value] : Manifest,
                 Format <: InputFormat[Key, Value] : Manifest](
                   configurationTemplate:Configuration, name:String, partitions:Int, step:Int, steps:Int)
  extends ComposablePartitionedIterativeJob[Key, Value, Mapper, Reducer, Partitioner, Format](
    configurationTemplate, name, partitions, step, steps) { }