package org.brandonhaynes

/** Classes that realize a single-source breadth-first search (SS-BFS) over a graph in adjacency form using one of
  * three algorithms:
  *
  *    - [[org.brandonhaynes.bfs.NaiveBFS]]: Perform SS-BFS using a naive, straightforward algorithm
  *                                          (see Lin & Dyer 2010)
  *    - [[org.brandonhaynes.bfs.SchimmyBFS]]: Perform SS-BFS using the Schimmy pattern (Lin & Dyer 2010)
  *    - [[org.brandonhaynes.bfs.TIMRBFS]]: Perform SS-BFS using temporal-iterative map/reduce (Haynes 2014)
  *
  * Input graphs are assumed to be in adjacency form that may be parsed as [[org.brandonhaynes.bfs.VertexMetadata]].
  * For example:
  *
  * {{{
  *   1000  (0, [1001, 1002])
  *   1001  []
  *   1002  [1003]
  *   1003  [1001]
  * }}}
  *
  * Here the distance `0` associated with vertex 1000 represents the origin of the SS-BFS; all other nodes are assumed
  * to have unknown distance.  During each iteration of the iterative process, the search frontier will be expanded
  * by one unit (e.g., after two iterations the value associated with vertex 1003 will be `(2, [1001])`).
  */
package object bfs { }
