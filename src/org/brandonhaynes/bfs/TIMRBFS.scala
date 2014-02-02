package org.brandonhaynes.bfs

import scala.language.reflectiveCalls
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.mapreduce.{Mapper, Job}
import org.brandonhaynes.timr.checkpoints.{ConfiguredCheckpoint, CheckpointTIMRReducer, CheckpointTIMRJob}
import org.brandonhaynes.timr.TIMRReduceContext._
import org.brandonhaynes.support.{TemporalPair, Implicits}
import org.brandonhaynes.bfs.Vertex._
import Implicits._

/** A tool for executing a TIMR-enabled SS-BFS over an input graph
  *
  * Sample command line:
  * {{{
  * hadoop jar SS-BFS.jar -input hdfs:///myGraph
  *                       -D localOutputPath=file:///aLocalPath
  *                       -D verbose=true
  *                       -D partitions=2
  *                       -D steps=3
  *                       -D checkpoints=10,15
  *                       -D algorithm=timr
  * }}}
  *
  * Note that a comma-separated list of checkpoint steps may be (optionally) specified, during with previous
  * pairs are rolled up and aggregated on the DFS for performance purposes.  This limits the cost of jobs with
  * large numbers of iterations.
  */
class TIMRBFS extends BFSTool {
  /** Create a new SS-BFS TIMR-enabled job */
  override def createJob(configuration:Configuration, name:String, partitions:Int, steps:Int)(step:Int) =
    new CheckpointTIMRJob[VertexId, VertexMetadata, TIMRBFSMapper, TIMRBFSReducer, BFSKeyPartitioner, BFSInputFormat](
      configuration, name, partitions, step, steps):Job
}

/** A mapper for TIMR-enabled SS-BFS over a graph */
class TIMRBFSMapper extends Mapper[VertexId, VertexMetadata, VertexId, VertexMetadata] {
  /** Should this pair be emitted into the intermediate phase? */
  private def shouldEmitPair(step:Long, distance:Option[Long]) = step > 0 || (step == 0 && distance == Some(0L))

  /** Mapping function for SS-BFS under TIMR */
  override def map(key:VertexId, vertex:VertexMetadata,
                   context:Mapper[VertexId, VertexMetadata, VertexId, VertexMetadata]#Context) =
    // Only emit a pair if it's an "interior" step, or the first step AND the initial vertex
    if(shouldEmitPair(context.step, vertex.distance))
      vertex.adjacencies.filterNot(_ == key.id).foreach(context.write(_, Nil))
}

/** A reducer for TIMR-enabled SS-BFS over a graph */
class TIMRBFSReducer extends CheckpointTIMRReducer[VertexId, VertexMetadata]()(Ordering.by(_.id)) {
  /** Pick up the checkpoint from configuration ("-D checkpoints=n,m,...") */
  override val checkpointMethod = new ConfiguredCheckpoint[VertexId, VertexMetadata, VertexId, VertexMetadata]

  /** Get the adjacency list from a traversable sequence of temporal pairs
    * (the adjacencies are always found on the first temporal pair, as input to the job)
    * @param previous Sequence of temporal pairs for this key
    * @return Adjacency list for the current vertex
    */
  private def getAdjacencies(previous:Traversable[TemporalPair[VertexMetadata]]) =
    previous.headOption.map(_.value.adjacencies).getOrElse(List.empty)

  /** Should the current vertex be emitted into the next step?
    * (only if there was an intermediate pair and we haven't already seen an intermediate!)
    * @param previous Previous temporal pairs for the iterative job
    * @param intermediate Current temporal pairs for the step
    * @return True when the pair should be emitted to the next step; false otherwise
    */
  private def shouldEmitPair(previous:Traversable[TemporalPair[VertexMetadata]],
                             intermediate:Traversable[TemporalPair[VertexMetadata]]) =
    !intermediate.isEmpty && previous.hasSingleElement && !previous.head.value.distance.isDefined

  /** Gets the final (best) SS-BFS distance encountered across all steps of the iterative job
    * @param previous Previous temporal pairs for the iterative job
    * @param intermediate Current temporal pairs for the step
    * @return Best distance encountered during the iterative job, if any
    */
  private def getFinalDistance(previous:Traversable[TemporalPair[VertexMetadata]],
                               intermediate:Traversable[TemporalPair[VertexMetadata]]) =
    // If we transitioned once during prior steps, just grab its initial-valid index
    if(previous.hasMultipleElements) Some(previous.last.interval.start)
    // Otherwise use the original/input distance, if any
    else if(previous.nonEmpty && previous.head.value.distance.isDefined) previous.head.value.distance.map(_.toInt)
    // Otherwise use the intermediate valid-interval, if any
    else intermediate.headOption.map(_.interval.start)

  /** Gets a key/value pair used in the final output for the iterative job */
  private def getFinalPair(key:VertexId, previous:Traversable[TemporalPair[VertexMetadata]],
                           intermediate:Traversable[TemporalPair[VertexMetadata]]) =
    (key, new VertexMetadata(getFinalDistance(previous, intermediate), getAdjacencies(previous)))

  /** During a checkpoint operation, summarize the prior temporal pairs by constructing a "current" final pair */
  override def summarize(context:Context)(key:VertexId, values:Iterator[TemporalPair[VertexMetadata]]) =
    getFinalPair(key, values.toStream, List.empty)

  /** Reduction function for TIMR-enabled SS-BFS */
  override def reduce(key:VertexId, previous:Iterator[TemporalPair[VertexMetadata]],
                      intermediate:Iterator[TemporalPair[VertexMetadata]], context:Context) = {
    (previous.toStream, intermediate.toStream) match {
      case (previousStream, intermediateStream) =>
        // If the last step, output final pairs and abandon TIMR-terseness
        if(context.isLastStep)
          context.write(getFinalPair(key, previousStream, intermediateStream))
        // Otherwise only emit the pair to the next step if it's been modified
        else if(shouldEmitPair(previousStream, intermediateStream))
          context.write(key, getAdjacencies(previousStream))
    }
  }
}