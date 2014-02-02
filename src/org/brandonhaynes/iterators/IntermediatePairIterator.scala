package org.brandonhaynes.iterators

import scala.language.{implicitConversions, reflectiveCalls}
import org.apache.hadoop.mapreduce.ReduceContext
import org.brandonhaynes.support.Implicits._

/** Implicit helper method for getting an iterator containing the pairs resulting from the intermediate shuffle
  *
  * Usage: {{{ context.intermediatePairs }}}
  */
object IntermediatePairIterator {
  /** Implicit conversion between a Hadoop reducer context and an object exposing the iteration of intermediate pairs */
  implicit def toIntermediateIterator[Key, Value, OutputKey, OutputValue](
      context:ReduceContext[Key, Value, OutputKey, OutputValue]) =
    new { def intermediatePairs:Iterator[(Key, Value)] = new IntermediatePairIterator(context) }
}

/** A class that iterator the intermediate pairs drawn from a Hadoop reducer context
 *
 * @param context Hadoop context over which to iterate
 * @tparam Key Key used in the Hadoop job
 * @tparam Value Value used in the Hadoop job
 */
class IntermediatePairIterator[Key, Value, OutputKey, OutputValue](
    context:ReduceContext[Key, Value, OutputKey, OutputValue])
  extends Iterator[(Key, Value)] {
    // Only iterate if we have a valid initial key
    private var _hasNext = context.getCurrentKey match {
      case reference:AnyRef => reference != null
      case _ => true
    }

    def hasNext = _hasNext

    def next() = {
      if(hasNext) {
        // Save our result, iterate to the next Hadoop key/value pair, and return the saved result
        val result = (context.getCurrentKey.cloneIfWritable(context), context.getCurrentValue)
        _hasNext = context.nextKeyValue()
        result
      } else
        throw new NoSuchElementException
    }
  }
