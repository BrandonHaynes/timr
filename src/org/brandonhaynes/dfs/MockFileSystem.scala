package org.brandonhaynes.dfs

import java.io.File
import java.net.URI

/** Degenerate DFS strategy for unit testing */
class MockFileSystem extends DFS {
  // Assume that all DFS URIs contain exactly one file
  private val fileUri = new URI("/part-00000")

  override def list(uri: URI) =
    if (uri == fileUri) Stream(fileUri)
    else Stream()

  override def open(uri: URI) =
    if (uri == fileUri) (0 until 100).map(i => (i, i)).map(_.toString()).iterator
    else Iterator()

  override def get(dfsURI: URI, localUri: URI) = open(dfsURI)

  override def getAll(dfsPathURI: URI, localUri: URI) = {
    // Get the filename component of a URI
    def getFilename(uri:URI):String = new File(uri.getPath).getName
    // Given an absolute base URI and a relative name, combine the two into a qualified URI
    def makeURI(uri:URI, name:String) = new URI(uri.getScheme, uri.getUserInfo, uri.getHost, uri.getPort,
                                                uri.getPath + name, uri.getQuery, uri.getFragment)

    list(dfsPathURI).map(uri => get(uri, makeURI(uri, getFilename(uri)))).flatten.toIterator
  }

  override def put(sourceURI: URI, destinationURI: URI) = ()
}
