package org.brandonhaynes.timr

import scala.language.implicitConversions
import org.apache.hadoop.mapreduce._

/** Implicit helper functions for a Hadoop task context under a TIMR/Schimmy/iterative job
  *
  * Usage:
  * {{{
  *   val currentStep = hadoopContext.step
  *   val totalSteps = hadoopContext.steps
  *   hadoopContext.isFirstStep
  *   hadoopContext.partition
  *   hadoopContext.write((key, value)) // Tupled version of write
  *   // ... and others
  * }}}
  */
object TIMRReduceContext {
  /** Convert a JobContext into a TIMRContext with helper properties */
  implicit def toTIMRReduceContext[Key, Value](context:JobContext) =
    new TIMRReduceContext(context)

  /** Convert a TaskInputOutputContext into an object that exposes a tupled write convenience function */
  implicit def toTupleWritableContext[InputKey, InputValue, OutputKey, OutputValue](
    context:TaskInputOutputContext[InputKey, InputValue, OutputKey, OutputValue]) = new {
      def write(pair:(OutputKey, OutputValue)) = (context.write _).tupled(pair)
  }
}

/** Augments a Hadoop task context with properties that are useful in an iterative setting.
  *
  * Note that we *adapt* the job context so that it need only be converted once, and thereafter may be used as both
  * a Hadoop and job context.
  *
  * @param jobContext Underlying job context to adapt
  * @tparam Key Key associated with the partitioner of this job
  * @tparam Value Value associated with the partitioner of this job
  */
class TIMRReduceContext[Key, Value](jobContext:JobContext)
  extends JobContext(jobContext.getConfiguration, jobContext.getJobID) {

  /** Get the current step number of an iterative job */
  lazy val step = getOrThrow("step")
  /** Get the total number of steps in the iterative job */
  lazy val steps = getOrThrow("steps")
  /** Is this the first step of the iterative job? */
  lazy val isFirstStep = step == 0
  /** Is this the last step of the iterative job? */
  lazy val isLastStep = step == steps - 1

  /** Get the current partition number for this task */
  lazy val partition = getOrThrow("mapred.task.partition") // Is this safe under all potential partitioner mappings?
  /** Get the total number of partitions active in this task */
  lazy val partitions = getOrThrow("partitions")

  /** Give a key and value, identify its partition */
  def getPartition(key:Key, value:Value) =
    jobContext.getPartitionerClass.newInstance()
        .asInstanceOf[Partitioner[Key, Value]]
        .getPartition(key, value, partitions)

  /** Utility method that throw an exception if a (presumed-invalid) default is returned from the configuration */
  private def getOrThrow(key:String, invalidValue:Int = -1) =
    jobContext.getConfiguration.getInt(key, invalidValue) match {
      case `invalidValue` => throw new NoSuchElementException(key)
      case value => value
    }
}