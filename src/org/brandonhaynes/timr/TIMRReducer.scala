package org.brandonhaynes.timr

import java.lang.Iterable
import scala.language.{implicitConversions, reflectiveCalls}
import org.apache.hadoop.mapreduce.Reducer
import org.brandonhaynes.iterators.MergeIterator
import org.brandonhaynes.timr.TIMRReduceContext._
import org.brandonhaynes.dfs.PreviousOutputs._
import org.brandonhaynes.iterators.IntermediatePairIterator._
import org.brandonhaynes.iterators.GroupIterator._
import org.brandonhaynes.support.{TemporalPair, Interval}

/** Base class for a TIMR reducer.  This class handles aggregation of temporal pairs of all previous outputs and the
  * result of the intermediate shuffle.  These temporal pairs are presented as key-groups to the abstract temporal
  * reduce function, which must be implemented.

  * TIMR jobs differ from their ordinary counterparts by allowing, for each key, reduction over the values that were
  * output during prior steps of the iterative process.  So, if a reducer output the value v at some prior step of the
  * iterative job, it will (i) have access to that value v during all subsequent steps, and (ii) know the precise steps
  * during which the value v was emitted.
  *
  * In a traditional reduce function, a sequence of values is presented as an argument.  Under a TIMR reducer,
  * a sequence of temporal pairs is presented, where each pair contains a value and its associated validity interval.
  * This interval may be used to determine which steps the value was "valid" (i.e., not overwritten by some subsequent
  * value).
  *
  * In addition to the added utility by having access to the previous values associated with a given key throughout
  * the iterative process, TIMR increases algorithm performance under some types of iterative jobs.  It does so under
  * the temporal normal form (TNF) assumption -- no value will be emitted from a reducer if it is unchanged from the
  * most previously-emitted value.  It is important for any developer to observe this invariant to maximize performance.
  *
  * By aggregating prior values and assuming TNF, TIMR allows developers to avoid forward-propagating state that is
  * needed during subsequent states, and requires the shuffling only of MUTATIONS to values.  Accordingly, for
  * concrete implementations that have few mutations relative to the size of the data that must be forward-propagated,
  * TIMR may offer a substantial performance benefit.
  *
  * {{{
  * class MyReducer extends org.brandonhaynes.timr.TIMRReducer[MyKey, MyValue] {
  *   override def reduce(key:MyKey, values:Iterator[TemporalPair[MyValue]], context:Context) = {
  *     ...
  *   }
  * }
  *
  * // Or ...
  *
  * class MyReducer extends org.brandonhaynes.timr.TIMRReducer[MyKey, MyValue] {
  *   override def reduce(key:MyKey,
  *                       previous:Iterator[TemporalPair[MyValue]],
  *                       intermediate:Iterator[TemporalPair[MyValue]],
  *                       context:Context) = {
  *     ...
  *   }
  * }
  * }}}
  *
  * @param order Total ordering over the domain of keys
  * @tparam Key The key being reduced over
  * @tparam Value The value being reduced over
  */
abstract class TIMRReducer[Key, Value](implicit order: Ordering[Key]) extends Reducer[Key, Value, Key, Value] {

  type Context = Reducer[Key, Value, Key, Value]#Context

  /** Select the first component of a triple */
  private def selectKey[A,B,C](triple:(A,B,C)) = triple._1
  /** Select the second and third component of a triple */
  private def selectNonKey[A,B,C](triple:(A,B,C)) = (triple._2, triple._3)
  /** An explicit ordering by the (implicitly orderable) key of a pair */
  private[timr] def orderByKey[A,B](implicit order: Ordering[A]): Ordering[(A, B)] = Ordering.by(_._1)
  /** Helper function which flattens a (pair, int) pair */
  private[timr] def flatten(tuple:((Key, Value), Int)) = (tuple._1._1, tuple._1._2, tuple._2)

  /** Partial aggregation, function f in the paper; indicates which prior steps to aggregate */
  protected def aggregationSteps(context:Context) = 0 to context.step

  /** Generate sequence of previous outputs, one for each prior step (except as filtered by the aggregation function) */
  protected def previousStreams(context:Context):Seq[Iterator[(Key, Value)]] =
    context.previousPairs(aggregationSteps(context))

  /** One of the two TIMR overloads that should be implemented by an algorithm designer
    * This variant presents the prior streams and intermediate result as a single merged sequence
    *
    * If a consumer wishes to differentiate between the prior and current temporal stream, consider overloading
    * the alternative overload.
    *
    * @param key Key being reduced over
    * @param values Sequence of temporal pairs (both prior and current)
    * @param context Reducer context
    */
  protected def reduce(key:Key, values:Iterator[TemporalPair[Value]], context:Context):Unit = {
    // Split the value groups into a "previous" and "intermediate" set
    val (previous, intermediate) = values.span(_.interval.start != context.step+1)
    // And pass these streams to the other subreduce overload
    reduce(key, previous, intermediate, context)
  }

  /** This reduce overload allows an algorithm designer to leverage prior and intermediate results separately
    *
    * @param key Key being reduced over
    * @param previous Temporal pairs from PRIOR steps
    * @param intermediate Temporal pairs from CURRENT step
    * @param context Reducer context
    */
  protected def reduce(key:Key, previous:Iterator[TemporalPair[Value]], intermediate:Iterator[TemporalPair[Value]],
                       context:Context) = ()

  /** The default Hadoop runner invokes the reduce function once per key group.  Here we perform the normal setup/cleanup
    * operations, and invoke the TIMR-specific reduction function instead of iterating over the intermediates.
    */
  protected override def run(context:Context) = {
    setup(context)
    context.nextKey() // Initialize the pair stream
    reduce(context)   // Perform temporal reduction
    cleanup(context)
  }

  /** The TIMR reducer automatically performs the merging, grouping, and dispatch of the values associated with a key's
    * prior and intermediate outputs, and forwards these temporal groups to the subreduce function.
    */
  protected def reduce(context:Context):Unit = {
    // Per the paper, construct a merged, key-grouped sequence of the prior and intermediate values associated with keys
    val iterators = previousStreams(context) ++ Iterator.single(context.intermediatePairs)
    val merged = MergeIterator.mergeWithIndex(flatten _, iterators:_*)(orderByKey)
    val groups = prepare(merged)

    // For each resulting (key, previous-and-intermediate-values) group, forward it to the temporal reducer
    groups.foreach({ case (key, values) => reduce(key, values, context) })
  }

  /** A final, degenerate implementation for "traditional" reduction, which is instead realized through the other
    * reduce overloads. This function is not invoked, and should not be overridden.
    */
  protected override final def reduce(key:Key, values:Iterable[Value], context:Context) = {}

  /** Per the paper, group a pair sequence by key and map its step number onto a temporal interval
    *
    * @param pairs Sequence of (key, value, step-number) triples to convert into temporal pairs
    * @return Sequence of temporal pairs representing validity intervals for the given key/value pairs
    */
  private[timr] def prepare(pairs:Iterator[(Key, Value, Int)]):Iterator[(Key, Iterator[TemporalPair[Value]])] =
    pairs.groupBy(selectKey, selectNonKey)
         .map((map_intervals _).tupled)

  /** Per the paper, map each step number onto a temporal interval consisting of its validity start and end time
    *
    * @param key Key associated with the given values
    * @param values Sequence of values and step number associated with its initial validity
    * @return Sequence of temporal pairs for each value and its associated validity interval
    */
  private def map_intervals(key:Key, values:Iterator[(Value, Int)]) = {
    // Lift our values and append a single "None" onto the sequence
    val paddedValues = values.map(Option.apply) ++ Seq(None)
    // Now we can take a sliding window of exactly two elements, and ensure that every value will be encountered
    val stepPairs = paddedValues.sliding(2)

    // Map our sliding window onto temporal pairs; when an end value is available, use it for the right-bound
    // of our temporal pair.  When none is available, we generate an right-unbounded interval.
    (key, stepPairs.map {
      case Some((value, start)) :: Some((_, end)) :: _ => new TemporalPair(value, new Interval(start, end))
      case Some((value, start)) :: None :: _ => new TemporalPair(value, new Interval(start))
      case _ => throw new NoSuchElementException("Expected exactly two entries")
    })
  }
}