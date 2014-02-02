package org.brandonhaynes.support

/*
 * Class representing an interval with finite left-bound and potentially infinite right-bound.
 * The interval extends a Scala range, and is thereby valid only over the set of integers.
 */
class Interval(start:Int, end:Option[Int]=None) extends Range(start, end.getOrElse(Int.MaxValue), 1) {
  // Create an interval with a non-infinite right-bound
  def this(start:Int, end:Int) = this(start, Some(end))

  override def lastOption = end

  // Introduce some convenience functions for when the right-bound is potentially infinite
  def endOption = end
  def maxOption: Option[Int] = end
  def sizeOption = end.map(_ - start) // None === Undefined size

  override def toString() =
    if(end.isDefined) super.toString()
    else "Range(%d ... +inf)".format(start)
}
