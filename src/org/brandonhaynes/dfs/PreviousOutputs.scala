package org.brandonhaynes.dfs

import java.io.File
import scala.language.implicitConversions
import org.apache.hadoop.fs.Path
import org.apache.hadoop.mapreduce.lib.input.FileSplit
import org.apache.hadoop.mapreduce._
import org.brandonhaynes.timr.TIMRReduceContext._

/** A set of implicit helper methods for obtaining pairs from the outputs of previous steps of an iterative
  * map/reduce job.
  */
object PreviousOutputs {
  /** Object exposing helper methods for obtaining pairs from the outputs of previous steps
    *
    * Note that pairs are always drawn from the partition identified by the given context
    *
    * @param context A valid Hadoop task context
    * @tparam Key Output key associated with a prior step
    * @tparam Value Output value associated with a prior step
    * @return Object exposing helper methods
    */
  implicit def toPreviousIterator[Key, Value](context:TaskAttemptContext) = new {
    /** The output from the most recent step in the iterative job */
    def previousPairs:Iterator[(Key, Value)] = previousPairs(context.step)
    /** The output from a specific step */
    def previousPairs(step:Int):Iterator[(Key, Value)] =
      PreviousOutputs.previousPairs(context, step, context.partition)
    /** Output from a range of steps */
    def previousPairs(steps:Range):Seq[Iterator[(Key, Value)]] =
      steps.map(PreviousOutputs.previousPairs(context, _, context.partition))
  }

  /** Gets the output pairs associated with the given step and partition in the iterative job
    *
    * @param context Hadoop task context
    * @param step Step from which to gather output
    * @param partition Partition from which to gather output
    * @tparam Key Key used in the output of the given step
    * @tparam Value Value used in the output of the given step
    * @return Iteration of pairs from the output of the given step and partition
    */
  def previousPairs[Key, Value](context:TaskAttemptContext, step:Int, partition:Int):Iterator[(Key, Value)]=
    previousPairs(context, previousPairsFilename(context, step, partition))

  /** Gets the output associated with the given filename via the input format of the given Hadoop context
   *
   * @param context The Hadoop context from which to draw an input formatter
   * @param filename The filename from which to read
   * @tparam Key Input key
   * @tparam Value Input value
   * @return Iteration of pairs drawn from the given DFS file
   */
  private[dfs] def previousPairs[Key, Value](context:TaskAttemptContext, filename:Path):Iterator[(Key, Value)] = {
    // Create a degenerate file split and reader over that split
    val split = new FileSplit(filename, 0, Long.MaxValue, null)
    val formatter = context.getInputFormatClass.newInstance().asInstanceOf[InputFormat[Key, Value]]
    val reader = formatter.createRecordReader(split, context)

    reader.initialize(split, context)

    new PreviousPairsIterator(reader)
  }

  /** Get the filename associated with the given step and partition under the (given) Hadoop context
   *
   * @param context Hadoop context from which to draw paths
   * @param step Step number for the desired filename
   * @param partition Partition number for the desired filename
   * @return Qualified DFS path associated with the given step and partition output file
   */
  private[dfs] def previousPairsFilename(context:TaskAttemptContext, step:Int, partition:Int) = {
    val filenameFormat = if(step == 0) "part-%05d" else "part-r-%05d"
    val path = context.getConfiguration.getStrings("paths")(step)
    new Path(new File(path, filenameFormat.format(partition)).toString)
  }

  /** Class that iterates over a record reader and exposes its underlying pairs as an iterator */
  private class PreviousPairsIterator[Key, Value](reader: RecordReader[Key, Value]) extends Iterator[(Key, Value)] {
    private var _hasNext = reader.nextKeyValue()

    override def hasNext = _hasNext

    override def next() = {
      val current = (reader.getCurrentKey, reader.getCurrentValue)
      _hasNext = reader.nextKeyValue()
      if(!_hasNext) reader.close()
      current
      }

    def close() = reader.close()
  }
}