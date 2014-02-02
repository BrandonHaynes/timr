package org.brandonhaynes.schimmy

import java.lang.Iterable
import scala.language.reflectiveCalls
import org.apache.hadoop.mapreduce.Reducer
import org.brandonhaynes.iterators.IntermediatePairIterator._
import org.brandonhaynes.iterators.MergeIterator._
import org.brandonhaynes.iterators.GroupIterator._
import org.brandonhaynes.dfs.PreviousOutputs._

/** Base class for a Schimmy reducer.  This class handles aggregation of the original input and the
  * result of the intermediate shuffle.  The resulting aggregated pairs presented to the reducer as normal.
  *
  * {{{
  * class MyReducer extends org.brandonhaynes.timr.SchimmyReducer[MyKey, MyValue] {
  *   override def reduce(context:Reducer[Key, Value, Key, Value]#Context)(key:Key, values:Iterator[Value]) = {
  *     ...
  *   }
  * }
  * }}}
  *
  * @tparam Key Key being reduced over
  * @tparam Value Value being reduced over
  */
abstract class SchimmyReducer[Key, Value]()(implicit order:Ordering[Key])
  extends Reducer[Key, Value, Key, Value] {

  private def selectKey(pair:(Key, Value)) = pair._1
  private def selectValue(pair:(Key, Value)) = pair._2
  private def orderByKey: Ordering[(Key, Value)] = Ordering.by(selectKey)

  protected def reduce(context:Reducer[Key, Value, Key, Value]#Context)(key:Key, values:Iterator[Value]):Unit

  final override def reduce(key:Key, values:Iterable[Value], context:Reducer[Key, Value, Key, Value]#Context) =
    (context.intermediatePairs, context.previousPairs)
      .merge(orderByKey)
      .groupBy(selectKey, selectValue)
      .foreach((reduce(context) _).tupled)
}