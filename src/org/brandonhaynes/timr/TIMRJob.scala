package org.brandonhaynes.timr

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.io.Writable
import org.apache.hadoop.mapreduce
import org.apache.hadoop.mapreduce.InputFormat
import org.brandonhaynes.schimmy.SchimmyJob

/** Base class for a temporal-iterative map/reduce (TIMR) job (Haynes, 2014).  TIMR jobs differ from their ordinary
  * counterparts by allowing, for each key, reduction over the values that were output during prior steps of the
  * iterative process.  So, if a reducer output the value v at some prior step of the iterative job, it will (i) have
  * access to that value v during all subsequent steps, and (ii) know the precise steps during which the value v
  * was emitted.
  *
  * In a traditional reduce function, a sequence of values is presented as an argument.  Under a TIMR reducer,
  * a sequence of temporal pairs is presented, where each pair contains a value and its associated validity interval.
  * This interval may be used to determine which steps the value was "valid" (i.e., not overwritten by some subsequent
  * value).
  *
  * In addition to the added utility by having access to the previous values associated with a given key throughout
  * the iterative process, TIMR increases algorithm performance under some types of iterative jobs.  It does so under
  * the temporal normal form (TNF) assumption -- no value will be emitted from a reducer if it is unchanged from the
  * most previously-emitted value.  It is important for any developer to observe this invariant to maximize performance.
  *
  * By aggregating prior values and assuming TNF, TIMR allows developers to avoid forward-propagating state that is
  * needed during subsequent states, and requires the shuffling only of MUTATIONS to values.  Accordingly, for
  * concrete implementations that have few mutations relative to the size of the data that must be forward-propagated,
  * TIMR may offer a substantial performance benefit.
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
class TIMRJob[Key <: Writable : Manifest,
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
  extends SchimmyJob[Key, Value, Mapper, Reducer, Partitioner, Format](
    configurationTemplate, name, partitions, step, steps) { }
