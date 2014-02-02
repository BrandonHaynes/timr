package org.brandonhaynes.timr.checkpoints

import org.apache.hadoop.mapreduce.TaskInputOutputContext

/** Trait for a checkpoint strategy, which is utilized during an "active checkpoint step" (as identified by the
  * isActiveStep function).  During an active checkpoint step, pairs are summarized and committed to a checkpoint
  * store via the commitPairs function.  Concrete implementation should persist these pairs in a manner useful for
  * the particular strategy, and be prepared to load the pairs from the (most recent) checkpoint via the getPairs
  * function.
  *
  * @tparam InputKey Type of key used as input to the current step
  * @tparam InputValue Type of value used as input to the current step
  * @tparam OutputKey Type of key used as output from the current step
  * @tparam OutputValue Type of value used as output from the current step
  */
trait CheckpointMethod[InputKey, InputValue, OutputKey, OutputValue] {
  /** Gets the step number associated with the last time this checkpoint method was active, if any
    *
    * @param context Context for the current job
    * @return Most immediately prior step associated with an active checkpoint; None if no checkpoint has been performed
    */
  def lastCheckpointStep(context:TaskInputOutputContext[InputKey, InputValue, OutputKey, OutputValue]):Option[Int]

  /** Indicate whether the current job is an active checkpoint step, and that prior results should be merged,
    * summarized, and committed to storage.
    *
    * @param context Context of the current job
    * @return True when the current job is an active checkpoint step; false otherwise
    */
  def isActiveStep(context:TaskInputOutputContext[InputKey, InputValue, OutputKey, OutputValue]):Boolean

  /** Get the pairs associated with the most recent checkpoint operation, if any.
    *
    * Concrete strategies should return an empty iterator if no checkpoint has been performed (lastCheckpointStep==None)
    * rather than throwing an exception.
    *
    * @param context Context of the current job
    * @return Iteration of pairs drawn from the most recent checkpoint step; an empty iterator if no checkpoint has
    *         yet been performed.
    */
  def getPairs(context:TaskInputOutputContext[InputKey, InputValue, OutputKey, OutputValue]):Iterator[(InputKey, InputValue)]

  /** Commits the given set of pairs to a checkpoint "file" (or in whatever manner is appropriate for the concrete
    * strategy).
    *
    * @param context Context for the current job
    * @param pairs Iteration of pairs to emit in a manner relevant for the concrete strategy
    */
  def commitPairs(context:TaskInputOutputContext[InputKey, InputValue, OutputKey, OutputValue],
                  pairs:Iterator[(OutputKey, OutputValue)]): Unit
}
