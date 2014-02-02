package org.brandonhaynes.bfs

import scala.language.reflectiveCalls
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.mapreduce.{Reducer, Mapper, Job}
import org.brandonhaynes.schimmy.{SchimmyJob, SchimmyReducer}
import org.brandonhaynes.bfs.Vertex._
import org.brandonhaynes.support.Implicits
import Implicits._

/** A tool for executing a Schimmy-enabled SS-BFS over an input graph
  *
  * Sample command line:
  * hadoop jar SS-BFS.jar -input hdfs:///myGraph
  *                       -D localOutputPath=file:///aLocalPath
  *                       -D verbose=true
  *                       -D partitions=2
  *                       -D steps=3
  *                       -D algorithm=schimmy
  */
class SchimmyBFS extends BFSTool {
  /** Create a new SS-BFS Schimmy-enabled job */
  override def createJob(configuration:Configuration, name:String, partitions:Int, steps:Int)(step:Int) =
    new SchimmyJob[VertexId, VertexMetadata, TIMRBFSMapper, TIMRBFSReducer, BFSKeyPartitioner, BFSInputFormat](
      configuration, name, partitions, step, steps):Job
}

/** A mapper for Schimmy-enabled SS-BFS over a graph */
class SchimmyBFSMapper extends Mapper[VertexId, VertexMetadata, VertexId, VertexMetadata] {
  /** Mapping function for SS-BFS under Schimmy */
  override def map(key:VertexId, vertex:VertexMetadata, context:Mapper[VertexId, VertexMetadata, VertexId, VertexMetadata]#Context) =
    // Only emit if we're dealing with a non-empty distance, and don't emit adjacencies
    vertex.distance match {
      case Some(distance) => vertex.adjacencies.foreach(context.write(_, distance + 1))
      case _ =>
    }
}

/** A reducer for Schimmy-enabled SS-BFS over a graph */
class SchimmyBFSReducer extends SchimmyReducer[VertexId, VertexMetadata]()(Ordering.by(_.id)) {
  /** Reduction function for Schimmy-enabled SS-BFS */
  override def reduce(context:Reducer[VertexId, VertexMetadata, VertexId, VertexMetadata]#Context)
                      (key:VertexId, values:Iterator[VertexMetadata]) =
    // Emit the key and (optional) minimum distance and whatever non-empty adjacencies are available
    context.write(key, values.reduceLeft((left, right) => ((left, right).map(_.distance).minOption,
                                                           (left, right).map(_.adjacencies).flatten)))
}
