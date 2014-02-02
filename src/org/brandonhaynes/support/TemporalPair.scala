package org.brandonhaynes.support

import scala.language.implicitConversions

/** Implicit conversions for temporal pairs */
object TemporalPair {
  /** Convert a temporal pair into an "ordinary" pair */
  implicit def temporalPairToTuple[A](temporalPair:TemporalPair[A]) = (temporalPair.value, temporalPair.interval)
}

/** Represents a pairing of some value alpha and a valid-interval for that value */
case class TemporalPair[A](value:A, interval:Interval)
