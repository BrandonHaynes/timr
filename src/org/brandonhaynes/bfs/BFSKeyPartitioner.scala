package org.brandonhaynes.bfs

import org.apache.hadoop.mapreduce.Partitioner

/** Simple key partitioner for SS-BFS over map/reduce.  Here we take the key id modulo the number of partitioners,
  * and don't attempt to balance the partitions (since we know nothing about our graph structure)
  */
class BFSKeyPartitioner extends Partitioner[VertexId, VertexMetadata] {
  override def getPartition(key:VertexId, value:VertexMetadata, numPartitions:Int):Int =
    (key.id % numPartitions).toInt
}