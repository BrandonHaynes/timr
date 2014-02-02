package org.brandonhaynes.support

import java.text.SimpleDateFormat
import java.util.Calendar
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path
import org.apache.hadoop.io.Writable
import org.apache.hadoop.mapreduce
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat
import org.apache.hadoop.mapreduce.{Job, InputFormat}

/** Exposes some sane defaults for iterative jobs */
object IterativeJob {
  /** Get a reasonable default name for a job (includes the current date and time) */
  def defaultJobName = "job_" + new SimpleDateFormat("yyyyMMddHHmmss").format(Calendar.getInstance.getTime)
  /** Get a reasonable output path for the job (as a subdirectory of the input) */
  def defaultOutputPath(configuration:Configuration) = configuration.get("input") + "/output"
}

/** Represents a job that is one step of an iterative map/reduce job.  Serves as a lightweight wrapper over a
  * standard Hadoop job by setting some properties (e.g., mapper, reducer, input format) specifying some sane defaults
  * (number of reduce tasks, job name) and setting configuration useful for iterative jobs (e.g., total number of steps
  * in the job, current step number).
  *
  * @param configurationTemplate Template for job configuration; these values are copied for the new job
  * @param name Name of the job
  * @param partitions Number of partitions in the job; None if unspecified
  * @param step Step number of this job within the iterative process
  * @param steps Total number of steps in the iterative process
  * @tparam InputKey Type of key input to this step of the job
  * @tparam InputValue Type of value input to this step of the job
  * @tparam IntermediateKey Intermediate key type for this step of the job (output from mapper, input to reducer)
  * @tparam IntermediateValue Intermediate value type for this step of the job (output from mapper, input to reducer)
  * @tparam OutputKey Key type output from this step of the job
  * @tparam OutputValue Value type output from this step of the job
  * @tparam Mapper Concrete Mapping type
  * @tparam Reducer Concrete reducer type
  * @tparam Format Input format for data fed to this step of the job
  */
class IterativeJob[InputKey <: Writable,
                   InputValue <: Writable,
                   IntermediateKey,
                   IntermediateValue,
                   OutputKey : Manifest,
                   OutputValue : Manifest,
                   Mapper <: mapreduce.Mapper[InputKey, InputValue, IntermediateKey, IntermediateValue] : Manifest,
                   Reducer <: mapreduce.Reducer[IntermediateKey, IntermediateValue, OutputKey, OutputValue] : Manifest,
                   Format <: InputFormat[InputKey, InputValue] : Manifest](
                 configurationTemplate:Configuration, name:String, partitions:Option[Int], step:Int, steps:Int)
  extends Job(new Configuration(configurationTemplate), name) {

  // Set some default configuration
  setConfiguration()
  if(partitions.isDefined) setNumReduceTasks(partitions.get)

  // Allow easy access to our paths
  val inputPath = getPath(step)
  val outputPath = getPath(step + 1)
  val intermediatePath = getConfiguration.get("intermediatePath", "")

  // Set the types that will be used by this job
  setJarByClass(manifest[Mapper].runtimeClass)
  setJarByClass(manifest[Reducer].runtimeClass)

  setOutputKeyClass(manifest[OutputKey].runtimeClass)
  setOutputValueClass(manifest[OutputValue].runtimeClass)

  setMapperClass(manifest[Mapper].runtimeClass.asInstanceOf[Class[Mapper]])
  setReducerClass(manifest[Reducer].runtimeClass.asInstanceOf[Class[Reducer]])
  setInputFormatClass(manifest[Format].runtimeClass.asInstanceOf[Class[Format]])

  // Initialize the input and output formats for the job (if running under file formats)
  FileInputFormat.addInputPath(this, inputPath)
  FileOutputFormat.setOutputPath(this, outputPath)

  /** Set initial configuration details for the job
    * (by default, these include the job name, step number, total number of steps, and the set of paths
    * that will be evaluated during the iterative process)
    */
  protected def setConfiguration() = {
    getConfiguration.set("name", name)
    getConfiguration.setInt("step", step)
    getConfiguration.setInt("steps", steps)
    getConfiguration.setStrings("paths", (0 to steps).map(getPath).map(_.toString):_*)
  }

  /** Given a (zero-based) step number n, gets the input path for step n (and accordingly the output path of
    * step n-1).  For a job with m steps, getPath(m) indicates the final output path for the entire job.
    *
    * @param step Zero-based step for which a path is desired
    * @return Input path associated with the desired step (equivalent to the output path from the prior step)
    */
  protected def getPath(step:Int) = {
    new Path(step match {
      case 0 => getConfiguration.get("input")
      case `steps` => getConfiguration.get("output")
      case _ => "%s/%s/step-%d".format(intermediatePath, name, step)
    })
  }
}

/** Represents a composable job that is one step of an iterative map/reduce job, where the key and value types are
  * the same across all steps of the job.  Serves as a lightweight wrapper over a standard Hadoop job by setting some
  * properties (e.g., mapper, reducer, input format) specifying some sane defaults (number of reduce tasks, job name)
  * and setting configuration useful for iterative jobs (e.g., total number of steps in the job, current step number).
  *
  * @param configurationTemplate Template for job configuration; these values are copied for the new job
  * @param name Name of the job
  * @param partitions Number of partitions in the job; None if unspecified
  * @param step Step number of this job within the iterative process
  * @param steps Total number of steps in the iterative process
  * @tparam Key Type of key used for the input, intermediate output, and final output for this step of the job
  * @tparam Value Type of key used for the input, intermediate output, and final output for this step of the job
  * @tparam Mapper Concrete Mapping type
  * @tparam Reducer Concrete reducer type
  * @tparam Format Input format for data fed to this step of the job
  */
class ComposableIterativeJob[Key <: Writable : Manifest, Value <: Writable : Manifest,
                             Mapper <: mapreduce.Mapper[Key, Value, Key, Value] : Manifest,
                             Reducer <: mapreduce.Reducer[Key, Value, Key, Value] : Manifest,
                             Format <: InputFormat[Key, Value] : Manifest](
    configurationTemplate:Configuration, name:String, partitions:Option[Int], step:Int, steps:Int)
  extends IterativeJob[Key, Value, Key, Value, Key, Value, Mapper, Reducer, Format](
    configurationTemplate, name, partitions, step, steps) { }
