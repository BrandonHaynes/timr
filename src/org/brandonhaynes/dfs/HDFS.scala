package org.brandonhaynes.dfs

import java.io.File
import java.net.URI
import scala.io.{BufferedSource, Source}
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.FileSystem
import org.apache.hadoop.fs.Path

/** Concrete DFS strategy that utilizes the Hadoop DFS for file operations */
class HDFS extends DFS {
  val configuration = new Configuration

  override def list(uri: URI) =
    FileSystem.get(uri, configuration)
              .listStatus(new Path(uri.toString))
              .map(_.getPath)
              .filter(_ != null)
              .map(_.toUri)
              .toStream

  override def open(uri: URI) =
    // Note that we do not explicitly copy the file locally, though Hadoop may do so on our behalf
    new BufferedSource(FileSystem.get(uri, configuration).open(new Path(uri))).getLines()

  override def get(dfsUri: URI, localUri: URI) = {
    val fileSystem = FileSystem.get(dfsUri, configuration)
    val path = new Path(dfsUri)

    // Copy the file locally
    if(fileSystem.isFile(path) || !new File(localUri).exists())
      fileSystem.copyToLocalFile(new Path(dfsUri), new Path(localUri))

    // Now stream that file
    if(fileSystem.isFile(path))
      Source.fromFile(localUri).getLines()
    else
      Iterator.empty
  }

  def get(dfsUri:URI, localPath:URI, localName:String):Iterator[String] =
    get(dfsUri, new URI(localPath.getScheme, localPath.getUserInfo,
                        localPath.getHost, localPath.getPort,
                        new File(localPath.getPath, localName).toString,
                        localPath.getQuery, localPath.getFragment))

  override def getAll(dfsPathUri: URI, localUri: URI) =
    list(dfsPathUri)
      .map(uri => get(uri, localUri, new File(uri.getPath).getName))
      .flatten
      .toIterator


  override def put(sourceUri: URI, destinationUri: URI) =
    FileSystem.get(destinationUri, configuration)
              .copyFromLocalFile(new Path(sourceUri), new Path(destinationUri))
}
