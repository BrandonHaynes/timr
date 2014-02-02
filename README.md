# Temporal-Iterative Map/Reduce (TIMR)

### Temporal-Iterative Map/Reduce Framework for temporal reduction under iterative Hadoop jobs

This library contains support for temporal-iterative map/reduce (TIMR; Haynes 2014), along with an implementation
for the Schimmmy pattern (Lin & Dyer 2010) under iterative map/reduce.  Developers who wish to leverage TIMR
may instantiate a sequence of TIMR jobs via [org.brandonhaynes.timr.TIMRJob] and base a reducer on
[org.brandonhaynes.timr.TIMRReducer].

For jobs with large numbers of iterations, a significant performance gain may be realized by periodic checkpointing;
see [org.brandonhaynes.timr.checkpoints] for more details.

TIMR jobs differ from their ordinary counterparts by allowing, for each key, reduction over the values that were
output during prior steps of the iterative process.  So, if a reducer output the value v at some prior step of the
iterative job, it will (i) have access to that value v during all subsequent steps, and (ii) know the precise steps
during which the value v was emitted.

In a traditional reduce function, a sequence of values is presented as an argument.  Under a TIMR reducer,
a sequence of temporal pairs is presented, where each pair contains a value and its associated validity interval.
This interval may be used to determine which steps the value was "valid" (i.e., not overwritten by some subsequent
value).

In addition to the added utility by having access to the previous values associated with a given key throughout
the iterative process, TIMR increases algorithm performance under some types of iterative jobs.  It does so under
the temporal normal form (TNF) assumption -- no value will be emitted from a reducer if it is unchanged from the
most previously-emitted value.  It is important for any developer to observe this invariant to maximize performance.

By aggregating prior values and assuming TNF, TIMR allows developers to avoid forward-propagating state that is
needed during subsequent states, and requires the shuffling only of MUTATIONS to values.  Accordingly, for
concrete implementations that have few mutations relative to the size of the data that must be forward-propagated,
TIMR may offer a substantial performance benefit.

```scala
class MyReducer extends org.brandonhaynes.timr.TIMRReducer[MyKey, MyValue] {
  override def reduce(key:MyKey, values:Iterator[TemporalPair[MyValue]], context:Context) = {
    ...
  }
}
```

... Or ...

```scala
class MyReducer extends org.brandonhaynes.timr.TIMRReducer[MyKey, MyValue] {
  override def reduce(key:MyKey,
                      previous:Iterator[TemporalPair[MyValue]],
                      intermediate:Iterator[TemporalPair[MyValue]],
                      context:Context) = {
    ...
  }
}
```