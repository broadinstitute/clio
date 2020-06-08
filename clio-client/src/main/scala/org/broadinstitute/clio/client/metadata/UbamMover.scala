package org.broadinstitute.clio.client.metadata
import java.net.URI

import org.broadinstitute.clio.client.dispatch.MoveExecutor.MoveOp
import org.broadinstitute.clio.transfer.model.ubam.{UbamExtensions, UbamMetadata}

class UbamMover extends MetadataMover[UbamMetadata] {
  override protected def moveMetadata(
    src: UbamMetadata,
    destination: URI,
    newBasename: Option[String],
    undeliver: Boolean
  ): (UbamMetadata, Iterable[MoveOp]) = {
    val dest = src.copy(
      ubamPath = src.ubamPath.map(
        MetadataMover.buildFilePath(
          _,
          destination,
          newBasename.map(_ + UbamExtensions.UbamExtension)
        )
      )
    )

    (dest, extractMoves(src, dest, _.ubamPath))
  }
}
