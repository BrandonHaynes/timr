package org.brandonhaynes.timr.checkpoints

import org.apache.hadoop.fs.{Path, PathFilter}

/** Input filter for jobs that utilize checkpointing.  Because (at least by default) checkpoint strategies
  * commit results to the DFS under the same location as the step output*, if we don't somehow filter these
  * DFS files during the next step, we'll wind up incorrectly mapping over these previous results.
  *
  * So, we ignore any of these checkpoint outputs in subsequent step inputs.
  *
  * * This allows us to take advantage of Hadoop's built-in "commit" mechanism, whereby outputs are emitted to a
  * temporary intermediate location and only written to the actual output directory after the task completes
  * successfully.
 */
private[checkpoints] class CheckpointPathFilter extends PathFilter {
  override def accept(path:Path) =
    // This should probably be more resilient to other potential strategies, but suffices for now
    !path.toString.contains("checkpoints")
}