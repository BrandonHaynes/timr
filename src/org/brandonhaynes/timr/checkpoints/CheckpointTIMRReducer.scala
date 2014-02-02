package org.brandonhaynes.timr.checkpoints

import org.brandonhaynes.iterators.MergeIterator
import org.brandonhaynes.timr.TIMRReducer
import org.brandonhaynes.support.TemporalPair
import org.brandonhaynes.dfs.PreviousOutputs._

/** This reducer type extends TIMR reduction by introducing a checkpoint operation, during which the merging of
  * prior pairs are summarized and emitted to the DFS.  This bounds the merging cost of future steps, since
  * the size of the merge heap is related to the number of steps being merged.
  *
  * @param order Total order over the domain of keys
  * @tparam Key The key being reduced over
  * @tparam Value The value being reduced over
  */
abstract class CheckpointTIMRReducer[Key, Value](implicit order: Ordering[Key])
  extends TIMRReducer[Key, Value] {

  /** The checkpoint strategy utilized by the underlying algorithm */
  val checkpointMethod:CheckpointMethod[Key, Value, Key, Value]

  /** A function used by an underlying algorithm that converts a sequence of values into a "summarized" pair
    *
    * @param context The current Hadoop reducer context
    * @param key Key to summarize
    * @param values Iteration of temporal values over which to summarize
    * @return A summarize pair written to a checkpoint
    */
  def summarize(context:Context)(key:Key, values:Iterator[TemporalPair[Value]]):(Key, Value)

  /** Extend the underlying TIMR reduction to perform a checkpoint when applicable, and then proceed as usual */
  override def reduce(context:Context) = {
    if(checkpointMethod.isActiveStep(context)) checkpoint(context)
    super.reduce(context)
  }

  /** We augment the default TIMR stream generation by omitting outputs prior to the most recent checkpoint */
  override def previousStreams(context:Context) =
    previousStreams(context, checkpointMethod.lastCheckpointStep(context))

  /** Generate the sequence of previous streams, circumscribed by the most recent checkpoint (if any) */
  private def previousStreams(context:Context, previousStep:Option[Int]) =
    // When a checkpoint has occurred, include it in the sequence and post-fill with empty iterators
    (previousStep match {
      // Note that step is zero-based, so we fill with previous-checkpoint-1 empty iterators
      case Some(step) => Seq(checkpointMethod.getPairs(context)) ++ Iterator.fill(step)(Iterator.empty)
      case None => Seq.empty
    }) ++
    // Now we can append the set of outputs occurring after the most recent checkpoint (or all, if no checkpoint yet)
    aggregationSteps(context)
      .filterNot(_ <= previousStep.getOrElse(Int.MinValue))
      .map(context.previousPairs)

  /** This function performs the actual checkpointing operation by merging & grouping previous outputs and writing
    * the summarized result to a checkpoint file (or as otherwise executed per the given checkpoint strategy)
    *
    * @param context Current Hadoop reducer context
    */
  protected def checkpoint(context:Context) = {
    // Merge and group previous streams
    val iterators = super.previousStreams(context)
    val merged = MergeIterator.mergeWithIndex(flatten _, iterators:_*)(orderByKey)
    val groups = prepare(merged)
    // Now summarize those groups
    val summary = groups.map((summarize(context) _).tupled)

    // ... and commit the summarized pairs per the checkpoint strategy
    checkpointMethod.commitPairs(context, summary)
  }
}