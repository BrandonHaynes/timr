package org.brandonhaynes.timr.checkpoints

import scala.language.reflectiveCalls
import org.apache.hadoop.mapreduce.TaskInputOutputContext
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs
import org.brandonhaynes.dfs.CheckpointOutputs._
import org.brandonhaynes.dfs.UncommittedCheckpointOutput._
import org.brandonhaynes.timr.TIMRReduceContext._
import org.brandonhaynes.support.Implicits._

/** Concrete checkpoint strategy that performs a checkpointing strategy during each of the explicitly-specified steps
  * of an iterative job.
  *
  * This strategy performs a checkpoint through use of the Hadoop MultipleOutputs class.
  *
  * Note that the checkpoint steps that are specified in the constructor are zero-based.
  *
  * @param checkpointSteps Sequence of ZERO-BASED steps during which a checkpoint operation is performed.
 */
class ExplicitCheckpoint[InputKey, InputValue, OutputKey, OutputValue](checkpointSteps:Int*)
  extends CheckpointMethod[InputKey, InputValue, OutputKey, OutputValue] {

  def lastCheckpointStep(context:TaskInputOutputContext[InputKey, InputValue, OutputKey, OutputValue]) =
    // When we have access to pairs that have not yet been written to the DFS, we can treat a currently-active
    // step as a "last-checkpointed" step.  When we do not have access to those uncommitted pairs, we are forced
    // to ignore the current step.  This does not affect functionality, but it does impact performance.
    context.supportsUncommittedPairs match {
      case true =>  checkpointSteps.filter(_ <= context.step).maxOption
      case false => checkpointSteps.filter(_ < context.step).maxOption
    }

  def isActiveStep(context:TaskInputOutputContext[InputKey, InputValue, OutputKey, OutputValue]) =
    checkpointSteps.contains(context.step)

  def getPairs(context:TaskInputOutputContext[InputKey, InputValue, OutputKey, OutputValue]) =
    lastCheckpointStep(context) match {
      // Get uncommitted pairs if we're allowed to do so
      case Some(previousStep) if previousStep == context.step => context.uncommittedCheckpointPairs
      // Otherwise load pairs from a whatever previous step we checkpointed on
      case Some(previousStep) => context.checkpointPairs(previousStep+1)
      case _ => Iterator.empty
    }

  def commitPairs(context:TaskInputOutputContext[InputKey, InputValue, OutputKey, OutputValue],
                  values:Iterator[(OutputKey, OutputValue)]) = {
    val output = new MultipleOutputs[OutputKey, OutputValue](context)

    values.foreach({ case (key, value) => output.write(key, value, context.checkpointFilename) })

    output.close()
  }
}