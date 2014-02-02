package org.brandonhaynes.iterators

import scala.language.implicitConversions
import scala.collection.mutable

/** Implicit conversions and helper functions for efficiently merging multiple iterators.  Each input iterator is
  * assumed to be totally ordered by the associated, implicit ordering.
  *
  * Each of the merge operations is designed to be space-efficient (in theta(n) space for n iterators), and does
  * not cache prior values as a sequence/traversable.
  *
  * Usage:
  *
  * (iterator1, iterator2).merge
  * MergeIterator.merge(iterator1, iterator2, iterator3)
  * MergeIterator.merge(v => v.property, iterator1, iterator2)
  * // ... etc.
  */
object MergeIterator {
  /** Implicit conversion to object that supports merging of a pair of iterators */
  implicit def toMergedIterator[A](pair:(Iterator[A], Iterator[A])) =
    new { def merge(implicit order: Ordering[A]):Iterator[A] = MergeIterator.merge[A](pair._1, pair._2) }

  /** Merge an array of iterators and yield the results as defined by the implicit ordering.
    *
    * @param iterators Array of iterators to merge; NOTE: each is assumed to already be totally ordered!
    * @param order A total ordering over the value space
    * @tparam A Underlying value yielded by the iterators
    * @return Single, merged, totally-ordered iterator
    */
  def merge[A](iterators: Iterator[A]*)(implicit order: Ordering[A]) =
    new MergeIterator[A, A](_._1, iterators:_*):Iterator[A]

  /** Merge an array of iterators and yield the results as defined by the implicit ordering.  Values in the resulting
    * iterator are mapped prior to being returned (but are still totally ordered by type A).
    *
    * Equivalent to merge(i1, i2, ..., in).map(itemSelector)
    *
    * @param itemSelector Mapping operation to apply to items in the merged iterator
    * @param iterators Array of iterators to merge; NOTE: each is assumed to already be totally ordered!
    * @param order A total ordering over the value space
    * @tparam A Underlying value yielded by the iterators
    * @return Single, merged, totally-ordered iterator
    */
  def merge[A, Result](itemSelector:A=>Result, iterators: Iterator[A]*)(implicit order: Ordering[A]) =
    new MergeIterator[A, Result](pair => itemSelector(pair._1), iterators:_*):Iterator[Result]

  /** Merge an array of iterators and yield the results as defined by the implicit ordering.  The merged result
    * contains both the item from the respective iterators and the index from which the item was drawn (specifically
    * pairs of A and Int).  This allows consumers to identify both the values contained in the inputs and the
    * index from which that value was drawn.
    *
    * @param iterators Array of iterators to merge; NOTE: each is assumed to already be totally ordered!
    * @param order A total ordering over the value space
    * @tparam A Underlying value yielded by the iterators
    * @return Single, merged, totally-ordered iterator
    */
  def mergeWithIndex[A](iterators: Iterator[A]*)(implicit order: Ordering[A]) =
    new MergeIterator[A, (A, Int)](identity, iterators:_*):Iterator[(A, Int)]

  /** Merge an array of iterators and yield the results as defined by the implicit ordering.  Values in the resulting
    * iterator are mapped prior to being returned (but are still totally ordered by type A).  The domain of the item
    * mapping function contains both the value from the original iterator and the index from which that value was
    * drawn.
    *
    * @param itemSelector Mapping operation to apply to items in the merged iterator
    * @param iterators Array of iterators to merge; NOTE: each is assumed to already be totally ordered!
    * @param order A total ordering over the value space
    * @tparam A Underlying value yielded by the iterators
    * @return Single, merged, totally-ordered iterator
    */
  def mergeWithIndex[A, Result](itemSelector:((A, Int)) => Result, iterators: Iterator[A]*)
                               (implicit order: Ordering[A]) =
    new MergeIterator[A, Result](itemSelector, iterators:_*):Iterator[Result]

  /** Class that supports the various iteration types exposed above */
  private class MergeIterator[A, Result](itemSelector: ((A, Int)) => Result,
                                         iterators: Iterator[A]*)(implicit order: Ordering[A])
    extends Iterator[Result] {

    // Maintain a heap of values and indices drawn from the respective iterators.  Note that we order only
    // over the value and index, since we have no ordering over the iterator itself.
    private val heap = new mutable.PriorityQueue[(A, Int, Iterator[A])]()(Ordering.by{ t => (t._1, t._2) }).reverse

    // Initialize our heap with the iterator arguments
    iterators.zipWithIndex.foreach((addWithIndex _).tupled)

    def hasNext = !heap.isEmpty

    def next() =
      // Extract the next pair, insert its tail back onto the heap (before returning that pair)
      extractPair match {
        case Some((item, index, iterator)) =>
          // Insert the tail of the iterator back onto the heap, if any values remain in the residual iterator
          createTriple(iterator, index) match {
            case Some(triple) => heap.enqueue(triple)
            case None =>
          }
          // Now just return the item that was popped from the heap
          itemSelector(item, index)
        case None => throw new NoSuchElementException
      }

    /** Add a new iterator to the heap */
    def add(iterator: Iterator[A]):Unit =
      addWithIndex(iterator, heap.length)

    /** Add an iterator to the heap with a specific index */
    def addWithIndex(iterator: Iterator[A], index:Int) =
      createTriple(iterator, index) match {
        case Some(triple) => heap.enqueue(triple)
        case None =>
      }

    /** Pull a pair from the heap, if it is nonempty */
    private def extractPair =
      if(heap.isEmpty) None else Some(heap.dequeue())

    /** Create a new heap entry, if the iterator is nonempty */
    private def createTriple(iterator: Iterator[A], index: Int) =
      if(iterator.hasNext)
        Some((iterator.next(), index, iterator))
      else
        None
  }
}