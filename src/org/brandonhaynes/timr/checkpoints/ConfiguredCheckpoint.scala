package org.brandonhaynes.timr.checkpoints

import org.apache.hadoop.mapreduce.{TaskAttemptContext, TaskInputOutputContext}

/** Concrete checkpoint strategy that performs a checkpointing strategy during each of the explicitly-specified steps
  * of an iterative job.  These steps are specified via job configuration as a comma-separated list (e.g., "10,20,30").
  *
  * By default, this strategy draws the set of explicit steps from the "checkpoints" property in the configuration;
  * however, this may be overridden via the checkpointConfigurationKey.
  *
  * Note that the checkpoint steps that are specified in the configuration are zero-based.
  *
  * @param checkpointConfigurationKey Key from which to draw explicit checkpoint steps.  The value of this property
  *                                   should contain a comma-separated list of ZERO-BASED step numbers, or be empty
  *                                   when no checkpointing is desired.
  */
class ConfiguredCheckpoint[InputKey, InputValue, OutputKey, OutputValue](
    checkpointConfigurationKey:String = "checkpoints")
  extends CheckpointMethod[InputKey, InputValue, OutputKey, OutputValue] {

  /** Retrieve a list of integers from the configuration */
  private def checkpointSteps(context:TaskAttemptContext) =
    context.getConfiguration.getStrings(checkpointConfigurationKey) match {
      case null => Array.empty[Int]
      case steps => steps.map(_.toInt)
    }

  def lastCheckpointStep(context:TaskInputOutputContext[InputKey, InputValue, OutputKey, OutputValue]) =
    new ContextualCheckpoint(context).lastCheckpointStep(context)
  def isActiveStep(context:TaskInputOutputContext[InputKey, InputValue, OutputKey, OutputValue]) =
    new ContextualCheckpoint(context).isActiveStep(context)
  def getPairs(context:TaskInputOutputContext[InputKey, InputValue, OutputKey, OutputValue]) =
    new ContextualCheckpoint(context).getPairs(context)
  def commitPairs(context:TaskInputOutputContext[InputKey, InputValue, OutputKey, OutputValue],
                  values:Iterator[(OutputKey, OutputValue)]) =
    new ContextualCheckpoint(context).commitPairs(context, values)

  /** Create a new ExplicitCheckpoint instance using the list of steps specified in configuration */
  private class ContextualCheckpoint(context:TaskInputOutputContext[InputKey, InputValue, OutputKey, OutputValue])
    extends ExplicitCheckpoint[InputKey, InputValue, OutputKey, OutputValue](checkpointSteps(context):_*) { }
}
