package org.brandonhaynes.bfs

import org.apache.hadoop.mapreduce._
import org.apache.hadoop.mapreduce.lib.input.{KeyValueLineRecordReader, FileInputFormat}

/** An input format for SS-BFS over map/reduce.  Converts each line read from the record reader into
  * a vertex id and metadata (best distance and adjacencies)
  */
class BFSInputFormat extends FileInputFormat[VertexId, VertexMetadata] {
  override def createRecordReader(split:InputSplit, context:TaskAttemptContext) =
    new RecordReader[VertexId, VertexMetadata]() {
      val reader = new KeyValueLineRecordReader(context.getConfiguration)

      override def initialize(split:InputSplit, context:TaskAttemptContext) = reader.initialize(split, context)
      override def close() = reader.close()

      override def getCurrentKey = new VertexId(reader.getCurrentKey.toString.toLong)
      override def getCurrentValue = new VertexMetadata(reader.getCurrentValue)
      override def getProgress = reader.getProgress
      override def nextKeyValue() = reader.nextKeyValue()
    }
}