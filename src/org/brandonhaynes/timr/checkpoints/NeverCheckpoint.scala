package org.brandonhaynes.timr.checkpoints

import org.apache.hadoop.mapreduce.TaskInputOutputContext

/** Degenerate checkpoint strategy that never performs any checkpointing */
class NeverCheckpoint[InputKey, InputValue, OutputKey, OutputValue]
  extends CheckpointMethod[InputKey, InputValue, OutputKey, OutputValue] {

  def lastCheckpointStep(context:TaskInputOutputContext[InputKey, InputValue, OutputKey, OutputValue]) = None
  def isActiveStep(context:TaskInputOutputContext[InputKey, InputValue, OutputKey, OutputValue]) = false
  def getPairs(context:TaskInputOutputContext[InputKey, InputValue, OutputKey, OutputValue]) = Iterator.empty
  def commitPairs(context:TaskInputOutputContext[InputKey, InputValue, OutputKey, OutputValue],
                  pairs:Iterator[(OutputKey, OutputValue)]) = ()
}
