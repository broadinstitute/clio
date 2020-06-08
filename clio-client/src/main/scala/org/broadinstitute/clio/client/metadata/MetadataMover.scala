package org.broadinstitute.clio.client.metadata
import java.io.File
import java.net.URI

import org.broadinstitute.clio.client.dispatch.MoveExecutor.{IoOp, MoveOp}
import org.broadinstitute.clio.transfer.model.Metadata

import scala.collection.immutable

trait MetadataMover[M <: Metadata[M]] {

  final def moveInto(
    src: M,
    destination: URI,
    newBasename: Option[String] = None,
    undeliver: Boolean = false
  ): (M, immutable.Seq[IoOp]) = {
    if (!destination.getPath.endsWith("/")) {
      throw new IllegalArgumentException(
        s"Non-directory destination '$destination' given for metadata move"
      )
    }

    moveMetadata(src, destination, newBasename, undeliver) match {
      case (metadata, ops) => (metadata, ops.to[immutable.Seq])
    }
  }

  protected def moveMetadata(
    src: M,
    destination: URI,
    newBasename: Option[String],
    undeliver: Boolean
  ): (M, Iterable[IoOp])

  protected def extractMoves(
    original: M,
    moved: M,
    getPaths: M => Iterable[URI]
  ): Iterable[MoveOp] =
    getPaths(original).zip(getPaths(moved)).map(MoveOp.tupled)
}

object MetadataMover {

  /**
    * Given a `source` path, a `destination` directory, and an extension, figure out what
    * the new file name should be were the source file to be moved into the destination path,
    * optionally changing the base-name in the process.
    */
  def buildFilePath(
    source: URI,
    destination: URI,
    replacementName: Option[String] = None
  ): URI =
    destination.resolve(replacementName.getOrElse(new File(source.getPath).getName))
}
