package org.brandonhaynes.dfs

import scala.language.{implicitConversions, reflectiveCalls}
import org.apache.hadoop.fs.Path
import org.apache.hadoop.mapreduce._
import org.apache.hadoop.mapreduce.lib.output.{TextOutputFormat, FileOutputFormat}
import org.brandonhaynes.timr.TIMRReduceContext._
import org.brandonhaynes.dfs.PreviousOutputs._

/** A set of implicit helper methods for obtaining pairs from checkpointed DFS files (and uncommitted checkpoints) */
object CheckpointOutputs {
  // Filename format for checkpoint outputs
  val checkpointFilenameFormat = "checkpoints/checkpoint-%05d"

  /** Object exposing helper methods for obtaining pairs from the outputs of checkpoints
    *
    * Note that pairs are always drawn from the partition identified by the given context
    *
    * @param context A valid Hadoop task context
    * @tparam Key Output key associated with a prior step
    * @tparam Value Output value associated with a prior step
    * @return Object exposing helper methods
    */
  implicit def toPreviousIterator[Key, Value](context:TaskAttemptContext) = new {
    /** The name of the checkpoint file for the current partition */
    lazy val checkpointFilename = checkpointFilenameFormat.format(context.partition)
    /** Iteration of pairs from the most recent checkpoint */
    def checkpointPairs(step:Int) = checkpointOutput(context, step, context.partition)
  }

  /** Obtain pairs from the a checkpoint took place at the given step and partition
   *
   * @param context Hadoop context from which to draw paths
   * @param step Step from which the checkpoint should be drawn (this step is assumed to have a valid checkpoint)
   * @param partition Partition from which to draw pairs
   * @tparam Key Underlying key type for pairs in the checkpoint file
   * @tparam Value Underlying value type for pairs in the checkpoint file
   * @return Iteration of pairs drawn from the checkpoint file
   */
  def checkpointOutput[Key, Value](context:TaskAttemptContext, step:Int, partition:Int):Iterator[(Key, Value)] =
    previousPairs(context, checkpointFilename(context, step, partition))

  /** Generate a qualified DFS path for the checkpoint file at the given step and partition */
  private def checkpointFilename(context:TaskAttemptContext, step:Int, partition:Int) =
    // It's a little sloppy to do a string replace here; might want to reconsider this
    new Path(previousPairsFilename(context, step, partition).toString
              .replace("part", checkpointFilenameFormat.format(partition)))
}

/** A set of implicit helper methods for obtaining pairs from the (not yet) committed result of a checkpoint.
  *
  * Hadoop stages pending files in a temporary directory until the task is marked successful, and only thereafter
  * moves those files into its permanent home.  So, if we want to read these pending results, we must draw them
  * from this temporary location.
  *
  * Note that only the FileOutputFormat methods support the notion of a temporary staging location, and we only
  * return pairs for this type of output formatter.  Consumers may test for this fact via a static Boolean property
  * before attempting to retrieve pairs.
  */
object UncommittedCheckpointOutput {
  import org.brandonhaynes.dfs.CheckpointOutputs._

  /** Object exposing helper methods for obtaining pairs from the uncommitted checkpoint of the current step
    *
    * Note that pairs are always drawn from the partition identified by the given context
    *
    * @param context A valid Hadoop task context
    * @tparam Key Output key associated with a prior step
    * @tparam Value Output value associated with a prior step
    * @return Object exposing helper methods
    */
  implicit def toUncommittedPairIterator[Key, Value](context:TaskInputOutputContext[_, _, Key, Value]) = new {

    /** Indicates whether the current job supports retrieval of uncommited pairs
      * (pairs that are currently being checkpointed, but have not yet been written to the final DFS destination)
      */
    lazy val supportsUncommittedPairs = context.getOutputFormatClass.newInstance() match {
      case _:FileOutputFormat[Key, Value] => true
      case _ => false
    }

    /** Retrieves the set of uncommitted checkpoint pairs for the current step
      * (these are pairs that are currently being checkpointed, but have not been written to the final DFS output
      * destination)
      */
    def uncommittedCheckpointPairs =
      supportsUncommittedPairs match {
        case true =>
          // Get the name of the file in the current working output directory
          val filename = FileOutputFormat.getPathForWorkFile(context, context.checkpointFilename, "")
          // Now get the pairs as usual, using the specific filename
          PreviousOutputs.previousPairs(context, filename)
        // Should probably support strategy registration for other cases
        case _ => throw new UnsupportedOperationException("Uncommited checkpoints not supported for " +
                                                          "non-FileOutputFormat output formats")
      }
  }
}