package org.brandonhaynes.bfs

import java.lang.Iterable
import scala.language.reflectiveCalls
import scala.collection.JavaConversions._
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.io.Text
import org.apache.hadoop.mapreduce._
import org.apache.hadoop.mapreduce.lib.input.{KeyValueLineRecordReader, FileInputFormat}
import org.brandonhaynes.support.{ComposableIterativeJob, Implicits}
import org.brandonhaynes.bfs.Vertex._
import Implicits._

/** Hadoop Tool for executing a naive BFS over a graph
  *
  * Sample command line:
  * hadoop jar SS-BFS.jar -input hdfs:///myGraph
  *                       -D localOutputPath=file:///aLocalPath
  *                       -D verbose=true
  *                       -D steps=3
  *                       -D algorithm=naive
  */
class NaiveBFS extends BFSTool {
  /** Create a new iterative map/reduce job with a naive mapper and reducer; allows specification of the number of
    * reduce tasks, if greater than zero
    */
  override def createJob(configuration:Configuration, name:String, partitions:Int, steps:Int)(step:Int) =
    new ComposableIterativeJob[VertexId, Text, NaiveBFSMapper, NaiveBFSReducer, NaiveBFSInputFormat](
      configuration, name, if(partitions > 0) Some(partitions) else None, step, steps):Job
}

/** Input format for the naive BFS algorithm */
class NaiveBFSInputFormat extends FileInputFormat[VertexId, Text] {
  override def createRecordReader(split:InputSplit, context:TaskAttemptContext) =
    new RecordReader[VertexId, Text] {
      val reader = new KeyValueLineRecordReader(context.getConfiguration)

      def initialize(split:InputSplit, context:TaskAttemptContext) = reader.initialize(split, context)
      def close() = reader.close()

      def getCurrentValue = reader.getCurrentValue
      def getCurrentKey = new VertexId(reader.getCurrentKey.toString.toLong)
      def getProgress = reader.getProgress
      def nextKeyValue = reader.nextKeyValue
    }
}

/** A mapper for ordinary SS-BFS over a graph */
class NaiveBFSMapper extends Mapper[VertexId, Text, VertexId, Text] {
  /** Mapping function for ordinary SS-BFS */
  override def map(key:VertexId, value:Text, context:Mapper[VertexId, Text, VertexId, Text]#Context) = {
    // First, write our own metadata to the intermediate shuffle
    context.write(key, value)

    // Now, for each adjacency, write the vertex's current distance + 1
    val vertex = new VertexMetadata(value)
    vertex.distance match {
      case Some(distance) => vertex.adjacencies.foreach(context.write(_, new VertexMetadata(distance + 1)))
      case _ =>
    }
  }
}

/** Reducer for ordinary BFS */
class NaiveBFSReducer extends Reducer[VertexId, Text, VertexId, Text] {
  /** Reduction function for ordinary SS-BFS */
  override def reduce(key:VertexId, values:Iterable[Text],
             context:Reducer[VertexId, Text, VertexId, Text]#Context) =
    // Write the pair (key, (best-distance-seen, all-adjacencies))
    context.write(key, values.map(new VertexMetadata(_))
                             .reduceLeft((left, right) => ((left, right).map(_.distance).minOption,
                                                           (left, right).map(_.adjacencies).flatten)))
}