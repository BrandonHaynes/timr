package org.brandonhaynes.iterators

import scala.language.{implicitConversions, reflectiveCalls}
import org.brandonhaynes.support.Implicits
import Implicits._

/**
  * A class that performs a grouping over a (sorted) iterator; unlike its Scala equivalent,
  * this class allows grouping over an iterator (rather than a Traversable) and thereby
  * requires only constant space.  This is useful in cases where the iterator contains
  * a very large number of elements.
  */
object GroupIterator {
  /** Implicit helpers to allow grouping over an arbitrary iterator */
  implicit def toGroupBy[A](iterator: Iterator[A]) = new {
    /** Group an iterator given a key and value selector function */
    def groupBy[Key, Value](keySelector: A => Key, valueSelector: A => Value) =
      new GroupIterator(iterator)(keySelector, valueSelector):Iterator[(Key, Iterator[Value])]

    /** Group an iterator given a key selector function; values remain unaffected */
    def groupBy[Key](keySelector: A => Key):Iterator[(Key, Iterator[A])] = groupBy(keySelector, identity)
    /** Group an iterator over unselected values */
    def group = groupBy(identity)
  }

  /**
    * A private class that groups the given iterator using the given selectors
    */
  private class GroupIterator[A, Key, Value](iterator: Iterator[A])(keySelector: A => Key, valueSelector: A => Value)
    extends Iterator[(Key, Iterator[Value])] {

    /** A private state class that encapsulates a current value, current key, and prior key */
    private case class State(cachedValue:Option[A], currentKey:Option[Key], previousKey:Option[Key]) {
      def this(value:Option[A], previousKey:Option[Key]) = this(value, value.map(keySelector), None) }
    /** An iteration of states for this grouping; traversing this iteration yields a sequence of (value, key,
      * previous-key) triples */
    private val states = new BufferedIterator[State] {
      /** The current state */
      var head = new State(iterator.nextOption(), None)
      def hasNext = iterator.hasNext
      /** Clears the current value from the state, and returns it (projected via the value selector) */
      def consumeValue() = {
        val current = head.cachedValue.get
        head = State(None, head.currentKey, head.previousKey)
        valueSelector(current)
      }
      /** Transitions to the next state */
      def next() = {
        val current = head
        head = new State(iterator.nextOption(), head.currentKey)
        current
      }
    }

    /** Does the current state represent the start of a new group? */
    private def isGroupStart = states.head.cachedValue.isDefined && states.head.currentKey != states.head.previousKey

    def hasNext = {
      // Transition to the beginning of the next group
      while (!isGroupStart && iterator.hasNext)
        states.next()
      isGroupStart
    }

    def next() = {
      // Ensure that we are at the beginning of a group
      if (!isGroupStart && !hasNext)
        throw new java.util.NoSuchElementException

      // Now return a new pair with the current key and an iterator over the group's values
      (states.head.currentKey.get,
       new Iterator[Value] {
         /** Store the current group key, to ensure that we stay within that group */
         val groupKey = states.head.currentKey

         /** A predicate that indicates if we remain in the "current" group */
         private def isSameGroup = states.head.currentKey == groupKey

         def hasNext = {
           // Transition until we have a current value (and remain within the same group)
           if (!states.head.cachedValue.isDefined && iterator.hasNext && isSameGroup)
             states.next()
           states.head.cachedValue.isDefined && isSameGroup
         }

         def next() = {
           if (hasNext)
             // Eat our current value and return it
             states.consumeValue()
           else
             throw new java.util.NoSuchElementException
         }
       })
    }
  }
}