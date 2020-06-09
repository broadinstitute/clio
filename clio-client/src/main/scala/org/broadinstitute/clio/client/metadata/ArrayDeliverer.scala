package org.broadinstitute.clio.client.metadata
import java.net.URI

import org.broadinstitute.clio.client.dispatch.MoveExecutor.{CopyOp, IoOp, MoveOp}
import org.broadinstitute.clio.transfer.model.arrays.{ArraysExtensions, ArraysMetadata}

class ArrayDeliverer extends MetadataMover[ArraysMetadata] {

  def idatsDir: String = "idats/"

  override protected def moveMetadata(
    src: ArraysMetadata,
    destination: URI,
    newBasename: Option[String]
  ): (ArraysMetadata, Iterable[IoOp]) = {
    import MetadataMover.buildFilePath

    val idatDestination = destination.resolve(idatsDir)

    val dest = src.copy(
      vcfPath = src.vcfPath.map(
        buildFilePath(
          _,
          destination,
          newBasename.map(_ + ArraysExtensions.VcfGzExtension)
        )
      ),
      vcfIndexPath = src.vcfIndexPath.map(
        buildFilePath(
          _,
          destination,
          newBasename.map(_ + ArraysExtensions.VcfGzTbiExtension)
        )
      ),
      gtcPath = src.gtcPath.map(
        buildFilePath(
          _,
          destination,
          newBasename.map(_ + ArraysExtensions.GtcExtension)
        )
      ),
      grnIdatPath = src.grnIdatPath.map(buildFilePath(_, idatDestination)),
      redIdatPath = src.redIdatPath.map(buildFilePath(_, idatDestination))
    )

    // If the Array has already been delivered, we want to move the idats instead of copying them.
    // This also covers the "undeliver" case
    val idatOp = if (src.workspaceName.forall(_.isEmpty)) {
      CopyOp.tupled
    } else {
      MoveOp.tupled
    }
    val ops = Iterable(
      extractMoves(src, dest, _.vcfPath),
      extractMoves(src, dest, _.vcfIndexPath),
      extractMoves(src, dest, _.gtcPath),
      src.grnIdatPath.zip(dest.grnIdatPath).map(idatOp),
      src.redIdatPath.zip(dest.redIdatPath).map(idatOp)
    ).flatten

    (dest, ops)
  }
}
