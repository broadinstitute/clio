package org.broadinstitute.clio.client.metadata

import java.net.URI

import org.broadinstitute.clio.client.dispatch.MoveExecutor.{CopyOp, IoOp, MoveOp}
import org.broadinstitute.clio.transfer.model.arrays.{ArraysExtensions, ArraysMetadata}

class ArrayUndeliverer extends MetadataMover[ArraysMetadata] {
  override protected def moveMetadata(
    src: ArraysMetadata,
    destination: URI,
    newBasename: Option[String]
  ): (ArraysMetadata, Iterable[IoOp]) = {
    import MetadataMover.buildFilePath

    val idatDestination = destination.resolve(ArrayDeliverer.IdatsDir)

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

    val ops = Iterable(
      extractMoves(src, dest, _.vcfPath),
      extractMoves(src, dest, _.vcfIndexPath),
      extractMoves(src, dest, _.gtcPath),
      src.grnIdatPath.zip(dest.grnIdatPath).map(MoveOp.tupled),
      src.redIdatPath.zip(dest.redIdatPath).map(MoveOp.tupled)
    ).flatten

    (dest, ops)
  }
}
