package org.brandonhaynes.bfs

import java.io.{DataInput, DataOutput}
import scala.language.implicitConversions
import org.apache.hadoop.io._

/** Implicit helpers useful for converting primitives into graph-compatible entities (ids and metadata) */
object Vertex {
  /** Implicitly convert a long value into a vertex id */
  implicit def toWritableId(id:Long) = new VertexId(id)

  /** Implicitly convert a long value into a vertex with that distance and no adjacencies */
  implicit def toWritableVertex(distance:Long) = new VertexMetadata(distance)
  /** Implicitly convert a traversable sequence of adjacencies into a vertex with no distance */
  implicit def toWritableVertex(adjacencies:TraversableOnce[Long]) = new VertexMetadata(adjacencies)
  /** Implicitly convert a distance/adjacency pair into a vertex */
  implicit def toWritableVertex(pair:(Option[Long], TraversableOnce[Long])) = new VertexMetadata(pair)

  /** Implicitly convert a Hadoop Text instance into a vertex */
  private[brandonhaynes] implicit def toWritableVertex(value:Text) = new VertexMetadata(value)
  /** Implicitly convert a vertex into a Hadoop Text instance */
  private[brandonhaynes] implicit def toText(metadata:VertexMetadata) = new Text(metadata.toString)
}

/** Utility object for parsing strings as vertices */
object VertexMetadata {
  // Vertices with defined distances, e.g. "(999, [1,2,3])"
  private val pairPattern = """^\((\d+),\W*\[((?:\d++(?:,\W*)?)*+)\]\)""".r
  // Vertices without defined distances, e.g., "[1,2,3]"
  private val adjacencyPattern = """^\[((?:\d++(?:,\W*+)?)*+)\]""".r

  /** Parse a vertex as one of the following forms
    *   Empty string === (None, [])
    *   Without defined distance [1,2,3] === (None, [1,2,3])
    *   With defined distance (999, [1,2,3]) === (999, [1,2,3])
    *
    * @param text String to parse
    * @return A valid vertex instance (empty if parse fails)
    */
  protected def parse(text:String):(Option[Long], List[Long]) =
    text match {
      case "" => (None, Nil)
      case pairPattern(distance, adjacencies) => parse(distance, adjacencies)
      case adjacencyPattern(adjacencies) => parse("", adjacencies)
      case _ => throw new IllegalArgumentException("Could not parse argument '%s'".format(text))
    }

  /** Parse a string containing a distance value and comma-separated list of adjacencies
    *
    * @param distanceText String containing a distance integer (or either "None" or "" if none)
    * @param adjacencyText Comma-separated list of adjacency identifiers
    * @return The pair (optional distance, list of adjacencies)
    */
  private def parse(distanceText:String, adjacencyText:String) =
    (if(!distanceText.isEmpty && distanceText != "None") Some(distanceText.toLong) else None,
     adjacencyText.split(",").map(_.trim).filterNot(_.isEmpty).map(_.toLong).toList)
}

/** Class that represents a vertex identifier, suitable for map/reduce graph algorithms.
  *
  * (This class is essentially a wrapper over a single integer value with good utility/helper function support.)
  *
  * Note that the identifier is mutable only to support the Hadoop writable interface, and only mutated through
  * the readFields function.  Care has been taken to limit the scope with which post-construction mutation of
  * the identifier may be performed.
  *
  * @param _id Vertex identifier
  */
class VertexId(private var _id:Long) extends WritableComparable[VertexId] {
  /** Create a vertex with an undefined id; use of this constructor should be avoided wherever possible */
  protected def this() = this(0)

  def id = _id

  protected override def write(output:DataOutput) = output.writeUTF(toString)
  protected override def readFields(input:DataInput) = _id = input.readUTF.toLong
  override def toString = id.toString
  override def hashCode = id.hashCode
  override def compareTo(other:VertexId) = id.compareTo(other.id)
  override def equals(other:Any) = other match {
    case that:VertexId => that.getClass == getClass && that.id == id
    case _ => false
  }
}

/** Class that represents the metadata associated with a vertex for use under SS-BFS.
  *
  * This class exists as a thin wrapper over a (Option[Long], Traversable[Long]) pair.
  *
  * Note that the metadata are mutable only to support the Hadoop writable interface, and only mutated through
  * the readFields function.  Care has been taken to limit the scope with which post-construction mutation of
  * the identifier may be performed.
  *
  * @param _distance Distance associated with the given vertex
  * @param _adjacencies Set of adjacencies for this vertex (multiple edges between vertices are allowed)
  */
class VertexMetadata(private var _distance:Option[Long], private var _adjacencies:List[Long]) extends Writable {
  /** Create a vertex with no known distance and no adjacencies */
  def this() = this(None:Option[Long], Nil)
  /** Create a vertex with the given (optional) distance and traversable set of adjacencies */
  def this(pair:(Option[Long], TraversableOnce[Long])) = this(pair._1, pair._2.toList)
  /* Create a vertex with the given (optional) integer distance and traversable set of adjacencies */
  def this(distance:Option[Int], adjacencies:TraversableOnce[Long]) = this(distance.map(_.toLong), adjacencies.toList)
  /** Create a vertex with no known distance and the traversable set of adjacencies */
  def this(adjacencies:TraversableOnce[Long]) = this(None:Option[Long], adjacencies.toList)
  /** Create a vertex with the given (known) distance and no adjacencies */
  def this(distance:Long) = this(Option(distance), Nil)
  /** Create a vertex by parsing the given string for metadata; see VertexMetadata.parse for valid formats */
  def this(text:String) = this(VertexMetadata.parse(text))
  /** Create a vertex by parsing the given Text instance for metadata; see VertexMetadata.parse for valid formats */
  def this(text:Text) = this(text.toString)

  lazy val distance = _distance
  lazy val adjacencies = _adjacencies

  def write(output:DataOutput) = output.writeUTF(toString)

  def readFields(input:DataInput) =
    VertexMetadata.parse(input.readUTF()) match {
      case (newDistance, newAdjacencies) =>
        _distance = newDistance
        _adjacencies = newAdjacencies
    }

  override def toString =
    (distance, adjacencies) match {
      case (None, Nil) => ""
      case (None, _) => "[" + adjacencies.mkString(",") + "]"
      case (Some(knownDistance), _) => "(%s, [%s])".format(knownDistance, adjacencies.mkString(","))
    }

  override def equals(other: Any) = other match {
    case that:VertexMetadata => that.getClass == getClass &&
                                  that.distance == distance &&
                                  that.adjacencies == adjacencies
    case _ => false
  }
  override def hashCode = 31 * (distance.hashCode ^ adjacencies.hashCode)
}