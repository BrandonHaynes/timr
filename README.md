# Temporal-Iterative Map/Reduce (TIMR)

### Temporal-Iterative Map/Reduce Framework for temporal reduction under iterative Hadoop jobs

This library contains support for temporal-iterative map/reduce (TIMR; Haynes 2014), along with an implementation
for the Schimmmy pattern (Lin & Dyer 2010) under iterative map/reduce.  Developers who wish to leverage TIMR
may instantiate a sequence of TIMR jobs via
[TIMRJob](http://brandonhaynes.github.io/timr/#org.brandonhaynes.timr.TIMRJob) and base a reducer on
[TIMRReducer](http://brandonhaynes.github.io/timr/#org.brandonhaynes.timr.TIMRReducer).

For jobs with large numbers of iterations, a significant performance gain may be realized by periodic checkpointing;
see
[org.brandonhaynes.timr.checkpoints](http://brandonhaynes.github.io/timr/#org.brandonhaynes.timr.checkpoints.package)
for more details.

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

### Single-Source Breadth-First Search (SS-BFS) under TIMR

To demonstrate the utility of TIMR under algorithms that mutate a small number of pairs relative to the size of the
 input, three implementations of SS-BFS are included: an "ordinary" implementation, one that utilizes the Schimmy
 pattern, and one that utilizes TIMR.  See
 [org.brandonhaynes.bfs](http://brandonhaynes.github.io/timr/#org.brandonhaynes.bfs.package) for more details.

 To execute under TIMR from the command line, select an algorithm and execute as:

 ```
hadoop jar TIMR.jar -input hdfs:///input-graph
                    -D localOutputPath=file:///a-local-path
                    -D verbose=true
                    -D partitions=2
                    -D steps=3
                    -D checkpoints=10,15
                    -D algorithm=timr
 ```

Note that the above implementation utilizes checkpointing; for optimal performance, these values should be adjusted
depending on the branching factor of the input graph.

Performance under a naive implementation may be contrasted via the following command line:

```
hadoop jar TIMR.jar -input hdfs:///input-graph
                    -D localOutputPath=file:///a-local-path
                    -D verbose=true
                    -D steps=3
                    -D algorithm=naive
 ```

Input graphs are assumed to be in adjacency form that may be parsed as
[VertexMetadata](http://brandonhaynes.github.io/timr/#org.brandonhaynes.bfs.VertexMetadata).  For example:

```
1000  (0, [1001, 1002])
1001  []
1002  [1003]
1003  [1001]
```

Here the distance `0` associated with vertex 1000 represents the origin of the SS-BFS; all other nodes are assumed
o have unknown distance.  During each iteration of the iterative process, the search frontier will be expanded
 by one unit (e.g., after two iterations the value associated with vertex 1003 will be `(2, [1001])`).
