package org.brandonhaynes.bfs

import java.net.URI
import scala.collection.JavaConversions._
import org.apache.hadoop.conf.{Configured, Configuration}
import org.apache.hadoop.fs.Path
import org.apache.hadoop.mapreduce.lib.jobcontrol.{JobControl, ControlledJob}
import org.apache.hadoop.mapreduce.Job
import org.apache.hadoop.fs.FileSystem
import org.apache.hadoop.util.{ToolRunner, Tool, GenericOptionsParser}
import org.apache.commons.cli.{Options, OptionBuilder}
import org.brandonhaynes.dfs.DFS
import org.brandonhaynes.support.IterativeJob

/** Implementation for SS-BFS under map/reduce, allowing specification of a concrete algorithm */
object BFS {
  // Map from command-line algorithm specification to concrete tool class */
  val algorithmMap = Map("algorithm=timr" -> (() => new TIMRBFS()),
                         "algorithm=schimmy" -> (() => new SchimmyBFS()),
                         "algorithm=naive" -> (() => new NaiveBFS()))

  /** Main class for SS-BFS under map/reduce */
  def main(arguments:Array[String]):Unit =
    // Grab the first valid algorithm specified on the command line, if any
    arguments.map(algorithmMap.get).filter(_.isDefined).map(_.get).headOption match {
      // Found one -- get a tool from the factory and run it
      case Some(factory) => ToolRunner.run(new Configuration(), factory(), arguments)
      // None found; default to TIMR
      case _ => ToolRunner.run(new Configuration(), new TIMRBFS(), arguments)
    }
}

/** Base class for tools that execute iterative SS-BFS under map/reduce
  *
  * Consumers need only specify a job factory (createJob below) in order to realize iterative SS-BFS.
  */
abstract class BFSTool extends Configured with Tool {
  val defaultSteps = 1
  val defaultPartitions = 1

  /** Job factory for SS-BFS jobs */
  def createJob(configuration:Configuration, name:String, partitions:Int, steps:Int)(step:Int):Job

  /** Load arguments from the command line, and push them into the job configuration */
  def loadArguments(arguments:Array[String]) = {
    val configuration = getConf
    val options = new Options()

    OptionBuilder.withArgName("input")
    OptionBuilder.hasArg
    OptionBuilder.isRequired
    OptionBuilder.withDescription("input file or path for job")
    options.addOption(OptionBuilder.create("input"))

    OptionBuilder.withArgName("output")
    OptionBuilder.hasArg
    OptionBuilder.withDescription("output path for job")
    options.addOption(OptionBuilder.create("output"))

    val parser = new GenericOptionsParser(configuration, options, arguments)
    if(parser.getCommandLine != null) {
      configuration.set("input", parser.getCommandLine.getOptionValue("input"))
      configuration.set("output", parser.getCommandLine.getOptionValue("output") match {
        case null => IterativeJob.defaultOutputPath(configuration)
        case path => path
      })
    }

    parser.getCommandLine != null
  }

  /** Tool runner for iterative SS-BFS under map/reduce */
  def run(arguments:Array[String]):Int = {
    if(!loadArguments(arguments))
      return 2

    // Get our job details, either specified explicitly or defaults
    val configuration = getConf
    val name = configuration.get("name", IterativeJob.defaultJobName)
    val steps = configuration.getInt("steps", defaultSteps)
    val partitions = configuration.getInt("partitions", defaultPartitions)
    val isVerbose = configuration.getBoolean("verbose", false)
    val overwriteOutput = configuration.getBoolean("overwriteOutput", true)
    val localOutputPath = Option(configuration.get("localOutputPath"))

    // Build a job controller and instantiate our jobs
    val controller = new JobControl(name + "-control")
    val jobFactory = createJob(configuration, name, partitions, steps) _
    val jobs = (0 until steps).reverse.foldLeft(List.empty[Job])((jobs, step) => jobFactory(step) :: jobs)
    val controlledJobs = jobs.map(_.getConfiguration).map(configuration => new ControlledJob(configuration))
    val dependencyPairs = controlledJobs.zip(controlledJobs.tail)

    if(overwriteOutput)
      FileSystem.get(configuration).delete(new Path(configuration.get("output")), true)

    // For each job and its antecedent, establish a dependency between the two
    dependencyPairs.foreach({ case (child, parent) => parent.addDependingJob(child) })
    controller.addJobCollection(controlledJobs)

    // Now run all the jobs, wait for their completion, and output (if necessary)
    (jobs.forall((job:Job) => controller.allFinished || job.waitForCompletion(isVerbose)), localOutputPath) match {
      case (true, Some(path)) => DFS.current.getAll(new URI(configuration.get("output")),
        new URI(path)).foreach(println); println(configuration.get("output")); 0
      case (true, _) => println("true"); 0
      case (false, _) => println("false"); 1
    }
  }
}