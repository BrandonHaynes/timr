package org.brandonhaynes.timr

/** This package contains support for checkpointing long-running TIMR jobs.  Because the cost of merging the outputs
  * of previous steps is (in part) related to the number of iterations, it is useful to periodically summarize
  * the sequence of values associated with each key and commit these merged results to a checkpoint file.
  *
  * The [[org.brandonhaynes.timr.checkpoints.CheckpointTIMRReducer]] performs this checkpoint merging automatically
  * via a checkpointing strategy (see [[org.brandonhaynes.timr.checkpoints.CheckpointMethod]]) specified by a developer.
 */
package object checkpoints { }
