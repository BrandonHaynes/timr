package org.brandonhaynes.dfs

import java.net.URI

/** Exposes a current DFS strategy
  *
  * This should be converted to utilize dependency injection at some point!
  */
object DFS {
  val current = new HDFS()
}

/** Trait that exposes the DFS operations necessary for TIMR & Schimmy. */
trait DFS {
  /** Lists the DFS files present under the given URI
    *
    * @param uri URI from which to list files
    * @return Stream of files present on the DFS under the given URI
    */
  def list(uri: URI): Stream[URI]

  /** Opens the DFS file at the given URI and returns its lines (as strings)
    *
    * @param uri URI from which to read
    * @return Iteration of strings contained in the DFS file
    */
  def open(uri: URI): Iterator[String]

  /** Copies the file located at the given URI to local storage, and returns its contents (as strings)
    *
    * @param dfsURI DFS location from which to copy data
    * @param localUri Local URI to which the DFS file is copied
    * @return Iteration of strings contained in the (now local) file
    */
  def get(dfsURI: URI, localUri: URI): Iterator[String]

  /** Copies all files present at the given DFS URI to local storage, and returns the aggregated results in arbitrary
    * order (as strings).
    *
    * @param dfsPathURI Directory (or file) on the DFS from which to copy files (file)
    * @param localUri Local directory URI to which files are copied
    * @return Iteration of strings contained in the (now local) files
    */
  def getAll(dfsPathURI: URI, localUri: URI): Iterator[String]

  /** Copies the given file to the DFS
    *
    * @param sourceURI Source URI to be copied (may be local or already on the DFS)
    * @param destinationURI Destination URI to which the file is copied
    */
  def put(sourceURI: URI, destinationURI: URI): Unit
}
