package org.brandonhaynes.support

import scala.language.{implicitConversions, reflectiveCalls}
import org.apache.hadoop.mapreduce.TaskAttemptContext
import org.apache.hadoop.io.{WritableUtils, Writable}

/** A set of implicit convenience functions */
private[brandonhaynes] object Implicits {
  /** Utilities that operate over iterators */
  implicit def toIteratorUtilities[A](iterator:Iterator[A]) = new {
    /** Transition an iterator if it has additional values and return the new value; return None if no values remain */
    implicit def nextOption() = if(iterator.hasNext) Some(iterator.next()) else None
  }

  /** Utilities that operate over Traversables */
  implicit def toTraversableUtilities[A](traversable:Traversable[A]) = new {
    /** Does the traversable contain multiple elements? */
    implicit lazy val hasMultipleElements = !traversable.drop(1).isEmpty
    /** Does the traversable contain a single element? */
    implicit lazy val hasSingleElement = !traversable.isEmpty && traversable.drop(1).isEmpty
  }

  /** Utilities that operate over pairs */
  implicit def toPairUtilities[A](pair:(A, A)) = new {
    /** Convert the pair to a sequence */
    implicit def toSeq = Seq(pair._1, pair._2)
    /** Perform a map over a pair */
    implicit def map[B](f:A=>B):(B, B) = (f(pair._1), f(pair._2))
    /** Given a pair of (same-valued) traversables, flatten that pair into a single combined traversable */
    implicit def flatten[B](implicit asTraversable:A=>TraversableOnce[B]) = Seq(pair._1, pair._2).flatten
  }

  /** Utilities that clone a writable */
  implicit def toClonableWritable[A <: Writable](value:A) = new {
    /** Clone a writable value */
    def cloneWritable(context:TaskAttemptContext) = WritableUtils.clone(value, context.getConfiguration)
  }

  /** Utilities that clone a value if it is writable, or returns it untransformed otherwise */
  implicit def toClonable[A](value:A) = new {
    /** Clone a value if it is writable; return it untransformed otherwise */
    def cloneIfWritable(context:TaskAttemptContext) = value match {
      case writable:Writable => writable.cloneWritable(context).asInstanceOf[A]
      case _ => value
    }
  }

  /** Min/Max utilities that operate over arbitrary sequences */
  implicit def toMinMaxOption[A](values:Seq[A]) =
    new MinMaxOption(values)
  /** Min/Max utilities that ignore "None" values in a sequence and then operate over the remaining valid values
    * maxOption(Seq(None, Some(1))) === Some(1), maxOption(Seq()) === None, maxOption(Seq(None)) === None
    */
  implicit def toMinMaxOptionOption[A](values:Seq[Option[A]]) =
    toMinMaxOption(values.filter(_.isDefined).map(_.get))
  /** Min/Max utilities that operate over arbitrary pairs */
  implicit def toMinMaxOptionOptionPair[A](pair:(A, A)) =
    toMinMaxOption(pair.toSeq)
  /** Min/Max utilities that operate over a pair of options */
  implicit def toMinMaxOptionPair[A](pair:(Option[A], Option[A])) =
    toMinMaxOptionOption(pair.toSeq)

  /** A helper class that exposes min and max options over a sequence of values.  Unlike the Scala equivalents,
    * this sequence may be empty (in which case the result is None).
    */
  class MinMaxOption[A](values:Seq[A]) {
    /** Return the maximum value of a sequence, if any; None otherwise */
    def maxOption(implicit order:Ordering[A]):Option[A] = applyIfNonEmpty(() => values.max)
    /** Return the minimum */
    def minOption(implicit order:Ordering[A]):Option[A] = applyIfNonEmpty(() => values.min)

    /** Return the minimum option of a sequence mapped over the given function */
    def minOptionBy[B](f:A=>B)(implicit order:Ordering[B]):Option[A] = applyIfNonEmpty(() => values.minBy(f))
    def maxOptionBy[B](f:A=>B)(implicit order:Ordering[B]):Option[A] = applyIfNonEmpty(() => values.maxBy(f))

    /** Applies a function f over the wrapped sequence if non-empty and returns its result; None otherwise */
    private def applyIfNonEmpty[B](f:()=>B) =
      values match {
        case Nil => None
        case applicable => Some(f())
      }
  }
}